"""资料流接收的持久化存储。

存储分三层，全部以追加或内容寻址方式写入，写入路径都带 fsync：

* objects/   原始信封与 Any 负载字节的内容寻址对象（SHA-256）；
* records.jsonl  来源序号 -> 原始对象 -> 描述符指纹 -> 结构化视图的登记；
* acks.jsonl     下游确认的只追加日志，重启或重播时据此判断是否重投。

“已交给下游”和“下游已确认”严格分开：只有确认日志落盘后才算确认完成，
因此重播时可能再次收到同一条消息，存储层按 (source, sequence) 幂等收敛，
未确认的记录保持 pending 并允许重新投递给下游。
"""

import hashlib
import json
import os
import tempfile
from pathlib import Path

from .archive import read_object

_RECORDS = 'records.jsonl'
_ACKS = 'acks.jsonl'
_RUNS = 'runs.jsonl'
_OBJECTS = 'objects'


def _fsync_append(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('ab') as writer:
        writer.write(payload)
        writer.flush()
        os.fsync(writer.fileno())


def _write_object(objects_dir, content):
    digest = hashlib.sha256(content).hexdigest()
    target = objects_dir / digest
    if not target.exists():
        objects_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=objects_dir, delete=False) as writer:
            temporary = Path(writer.name)
        try:
            temporary.write_bytes(content)
            temporary.replace(target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    return digest


class ReceiveStore:
    def __init__(self, directory):
        self.root = Path(directory)
        self.objects_dir = self.root / _OBJECTS
        self.records_path = self.root / _RECORDS
        self.acks_path = self.root / _ACKS
        self.runs_path = self.root / _RUNS
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self._records = self._load_jsonl(self.records_path)
        self._acks = self._load_jsonl(self.acks_path)
        self._index = {(item['source'], item['sequence']): item
                       for item in self._records}

    @staticmethod
    def _load_jsonl(path):
        if not path.exists():
            return []
        rows = []
        for line in path.read_text(encoding='utf-8').splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    # -- 原始对象 ------------------------------------------------------

    def put_object(self, content):
        return _write_object(self.objects_dir, bytes(content))

    def get_object(self, digest):
        return read_object(self.objects_dir, digest)

    # -- 记录 ----------------------------------------------------------

    def find(self, source, sequence):
        return self._index.get((source, sequence))

    def is_acked(self, source, sequence):
        key = (source, sequence)
        return any(row['source'] == source and row['sequence'] == sequence
                   for row in self._acks)

    def upsert_record(self, record):
        """按 (source, sequence) 幂等登记，重复原始字节不会产生第二条。

        返回 (record, existed)。重播时如果原始字节不同会直接报错——来源序号
        被复用且内容冲突属于协议异常，不能静默覆盖已经归档的原文。
        """
        key = (record['source'], record['sequence'])
        existing = self._index.get(key)
        if existing is not None:
            if existing['envelope_sha256'] != record['envelope_sha256']:
                raise ValueError(
                    '来源序号内容冲突：%s#%s' % key)
            return existing, True
        self._index[key] = record
        self._records.append(record)
        _fsync_append(self.records_path,
                      (json.dumps(record, ensure_ascii=False) + '\n').encode('utf-8'))
        return record, False

    def update_view(self, source, sequence, status, decoded, descriptor_id,
                    fingerprint, error=None):
        """重新解释后刷新结构化视图与描述符留痕，历史行保留在 jsonl 中。"""
        record = self._index[(source, sequence)]
        updated = dict(record)
        updated.update({
            'status': status,
            'descriptor_id': descriptor_id,
            'descriptor_fingerprint': fingerprint,
            'decoded': decoded,
            'decode_error': error,
        })
        self._index[(source, sequence)] = updated
        _fsync_append(self.records_path,
                      (json.dumps(updated, ensure_ascii=False) + '\n').encode('utf-8'))
        return updated

    def list_records(self):
        return list(self._index.values())

    # -- 确认 ----------------------------------------------------------

    def ack(self, source, sequence, downstream=None):
        if self.is_acked(source, sequence):
            return False
        entry = {
            'source': source,
            'sequence': sequence,
            'downstream': downstream,
        }
        self._acks.append(entry)
        _fsync_append(self.acks_path,
                      (json.dumps(entry, ensure_ascii=False) + '\n').encode('utf-8'))
        return True

    def list_acks(self):
        return list(self._acks)

    # -- 运行结果 ------------------------------------------------------

    def append_run(self, run):
        _fsync_append(self.runs_path,
                      (json.dumps(run, ensure_ascii=False) + '\n').encode('utf-8'))
        return run

    def list_runs(self):
        return self._load_jsonl(self.runs_path)
