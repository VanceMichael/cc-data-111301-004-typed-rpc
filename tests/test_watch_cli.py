import json
import subprocess
import sys
import tempfile
import unittest

from typed_rpc.rpc_source import rpc_source


class WatchCommandTests(unittest.TestCase):
    def _run(self, *arguments):
        process = subprocess.run(
            [sys.executable, '-m', 'typed_rpc', *arguments],
            capture_output=True, text=True, timeout=30)
        return process

    def test_watch_records_reinterpret_query_lifecycle(self):
        with tempfile.TemporaryDirectory() as store, rpc_source('fixtures') as address:
            watch = self._run('watch', address, '--store', store)
            self.assertEqual(watch.returncode, 0, watch.stderr)
            payload = json.loads(watch.stdout)
            self.assertEqual(payload['run']['ended'], 'completed')
            self.assertEqual(payload['run']['pending_descriptor'], 1)

            # 非 OK 结束状态必须以非零退出码上报。
            failed = self._run('watch', address, '--store', store, '--fail-end')
            self.assertEqual(failed.returncode, 2)
            self.assertEqual(json.loads(failed.stdout)['run']['ended'],
                             'rpc_error')

            # 结构化记录能反查到使用的描述符标识与内容指纹。
            queried = self._run('records', '--store', store, '--sequence', '1')
            record = json.loads(queried.stdout)['records'][0]
            self.assertEqual(record['descriptor_id'], 'v1')
            self.assertEqual(len(record['descriptor_fingerprint']), 64)

            # 补交原先未知的插件类型描述符，核对重新解释结果。
            reinterpreted = self._run(
                'reinterpret', '--store', store,
                '--descriptor-file', 'fixtures/unknown.pb',
                '--descriptor-id', 'unknown')
            self.assertEqual(reinterpreted.returncode, 0, reinterpreted.stderr)
            body = json.loads(reinterpreted.stdout)
            self.assertEqual(len(body['reinterpreted']), 1)
            state = body['reinterpreted'][0]['decoded']['fields']['state']['value']
            self.assertTrue(state['unknown_enum'])
            self.assertEqual(state['number'], 1)

            # 规范 JSON 视图带版本信封，64 位整数仍是字符串。
            canonical = self._run(
                'records', '--store', store, '--sequence', '1',
                '--canonical-json')
            envelope = json.loads(canonical.stdout)['records'][0]['decoded']
            self.assertEqual(envelope['canonical_version'], 1)
            self.assertEqual(envelope['view']['fields']['sequence']['value'],
                             '9007199254740994')

    def test_demo_watch_is_self_contained(self):
        process = self._run('demo-watch')
        self.assertEqual(process.returncode, 0, process.stderr)
        # demo-watch 打印 watch 汇总与重新解释汇总两个 JSON 对象。
        text = process.stdout
        decoder = json.JSONDecoder()
        objects = []
        index = 0
        while index < len(text):
            while index < len(text) and text[index] in ' \n\t':
                index += 1
            if index >= len(text):
                break
            obj, index = decoder.raw_decode(text, index)
            objects.append(obj)
        self.assertEqual(len(objects), 2)
        summary = objects[1]
        self.assertTrue(summary['canonical_json_roundtrip_lossless'])
        self.assertEqual(summary['still_pending'], [])
        self.assertEqual(len(summary['reinterpreted']), 1)


if __name__ == '__main__':
    unittest.main()
