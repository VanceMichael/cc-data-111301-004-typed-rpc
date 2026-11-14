"""有界、可取消、按来源描述符解释的 gRPC 资料流接收器。

设计要点：

* 读取线程只负责把消息放进有界队列；下游处理慢时队列满，读取线程阻塞，
  自然形成背压，不会无界堆积内存。
* 应用取消（CANCELLED）与服务端非 OK 结束都作为异常结束记录，绝不会被记成
  正常 completed；只有迭代器干净耗尽才是 completed。
* 先持久化原始信封和 Any 字节，再做动态解释；解释不了的类型保留原字节与
  type_url，登记为 pending_descriptor，补齐描述符后可重新解释。
* 下游确认单独落盘（见 store.ReceiveStore）。重播时已经确认的消息只做幂等
  归档、不再投递给下游；未确认的消息会重新投递。
"""

import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import grpc
from google.protobuf import any_pb2
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import message_factory

from .decoder import decode_any_payload
from .descriptors import UnknownTypeError, type_url_full_name


def _packet_class(packet_descriptor_path=None):
    """固定传输信封的动态类，不与插件描述符共用类型池。"""
    pool = descriptor_pool.DescriptorPool()
    pool.AddSerializedFile(any_pb2.Any.DESCRIPTOR.file.serialized_pb)
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.ParseFromString(
        _packet_descriptor_bytes(packet_descriptor_path))
    pool.AddSerializedFile(file_proto.SerializeToString())
    return message_factory.GetMessageClass(pool.FindMessageTypeByName('source.Packet'))


def _packet_descriptor_bytes(packet_descriptor_path=None):
    if packet_descriptor_path is None:
        packet_descriptor_path = Path(__file__).resolve().parent.parent / \
            'fixtures' / 'packet.pb'
    return Path(packet_descriptor_path).read_bytes()


@dataclass
class Delivery:
    """交给下游处理的一次消息投递。"""

    source: str
    sequence: str
    descriptor_id: str
    type_url: str
    envelope_sha256: str
    payload_sha256: str
    status: str
    decoded: object
    record: dict
    replay: bool = False

    def ack(self, store, downstream=None):
        return store.ack(self.source, self.sequence, downstream=downstream)


@dataclass
class RunResult:
    ended: str
    status_code: object = None
    details: str = None
    received: int = 0
    delivered: int = 0
    redelivered: int = 0
    acked: int = 0
    pending_descriptor: int = 0
    descriptor_ids: list = field(default_factory=list)
    error: str = None


class _LocalStreamError(Exception):
    """读取线程内部产生的、带 gRPC 状态码的流结束事件。"""

    def __init__(self, code, details):
        super().__init__(details)
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


class StreamClosed:
    """读取线程报告流正常耗尽的内部哨兵。"""


class StreamFailed:
    """读取线程报告流异常结束的内部哨兵。"""

    def __init__(self, error):
        self.error = error


class Receiver:
    def __init__(self, channel, registry, store, *, envelope_class=None,
                 queue_capacity=8, enqueue_timeout=10.0,
                 watch_method='/source.Catalog/Watch',
                 describe_method='/source.Catalog/Describe'):
        self.channel = channel
        self.registry = registry
        self.store = store
        self.envelope_class = envelope_class or _packet_class()
        self.queue_capacity = queue_capacity
        self.enqueue_timeout = enqueue_timeout
        self.watch_method = watch_method
        self.describe_method = describe_method
        self._cancel = threading.Event()
        self._active_call = None
        self._active_call_lock = threading.Lock()

    # -- 描述符 --------------------------------------------------------

    def preload_descriptors(self, descriptor_ids):
        """启动时尽力预取已知描述符；来源尚未发布的 NOT_FOUND 不算失败。"""
        loaded = []
        stub = self.channel.unary_unary(self.describe_method)
        for descriptor_id in descriptor_ids:
            try:
                raw = stub(descriptor_id.encode('utf-8'), timeout=5)
            except grpc.RpcError as error:
                if error.code() != grpc.StatusCode.NOT_FOUND:
                    raise
                continue
            bundle, _ = self.registry.register(descriptor_id, raw)
            loaded.append(bundle)
        return loaded

    def fetch_descriptor(self, descriptor_id):
        stub = self.channel.unary_unary(self.describe_method)
        raw = stub(descriptor_id.encode('utf-8'), timeout=5)
        bundle, _ = self.registry.register(descriptor_id, raw)
        return bundle

    # -- 取消 ----------------------------------------------------------

    def cancel(self):
        self._cancel.set()
        with self._active_call_lock:
            if self._active_call is not None:
                self._active_call.cancel()

    # -- 主流程 --------------------------------------------------------

    def run(self, request=b'{}', handler=None):
        result = RunResult(ended='running')
        bounded = queue.Queue(maxsize=self.queue_capacity)
        reader = threading.Thread(
            target=self._read_stream, args=(request, bounded), daemon=True)
        reader.start()
        try:
            self._consume(bounded, handler, result)
        except Exception as error:  # 下游/存储故障同样属于异常结束
            result.ended = 'error'
            result.status_code = grpc.StatusCode.INTERNAL
            result.error = repr(error)
            self.cancel()
            raise
        finally:
            reader.join(timeout=3)
            self.store.append_run({
                'ended': result.ended,
                'status_code': result.status_code.name
                if isinstance(result.status_code, grpc.StatusCode)
                else result.status_code,
                'details': result.details,
                'received': result.received,
                'delivered': result.delivered,
                'redelivered': result.redelivered,
                'acked': result.acked,
                'pending_descriptor': result.pending_descriptor,
                'descriptor_ids': list(result.descriptor_ids),
                'error': result.error,
                'finished_at': time.time(),
            })
        return result

    def _read_stream(self, request, bounded):
        call = self.channel.unary_stream(self.watch_method)(request)
        with self._active_call_lock:
            self._active_call = call
        try:
            for envelope_bytes in call:
                if self._cancel.is_set():
                    self._put_terminal(bounded, StreamFailed(_LocalStreamError(
                        grpc.StatusCode.CANCELLED, '接收已取消')))
                    return
                bounded.put(envelope_bytes, timeout=self.enqueue_timeout)
            self._put_terminal(bounded, StreamClosed())
        except queue.Full:
            # 下游长期不消费：主动结束并显式报告背压超时。
            call.cancel()
            self._put_terminal(bounded, StreamFailed(_LocalStreamError(
                grpc.StatusCode.RESOURCE_EXHAUSTED, '下游处理超时，接收队列已满')))
        except grpc.RpcError as error:
            self._put_terminal(bounded, StreamFailed(error))
        except Exception as error:  # pragma: no cover - 防御性分支
            self._put_terminal(bounded, StreamFailed(error))

    @staticmethod
    def _put_terminal(bounded, terminal):
        # 终止事件决定本次运行如何收尾，绝不能因队列满而丢失；消费者总会继续
        # get()，因此这里阻塞等待空位即可，且不会覆盖任何数据消息。
        while True:
            try:
                bounded.put(terminal, timeout=1.0)
                return
            except queue.Full:
                continue

    def _consume(self, bounded, handler, result):
        while True:
            item = bounded.get()
            if isinstance(item, StreamClosed):
                result.ended = 'completed'
                result.status_code = grpc.StatusCode.OK
                return
            if isinstance(item, StreamFailed):
                error = item.error
                result.ended = 'cancelled' if self._is_cancel(error) else 'rpc_error'
                result.status_code = getattr(error, 'code', lambda: None)()
                result.details = getattr(error, 'details', lambda: str(error))()
                result.error = repr(error)
                return
            delivery = self._handle_envelope(item, result)
            if delivery is None:
                continue
            already_acked = self.store.is_acked(delivery.source, delivery.sequence)
            if already_acked:
                # 重播且确认已持久化：幂等收敛，不再投递。
                continue
            if handler is not None:
                handler(delivery)
            result.delivered += 1
            if delivery.replay:
                result.redelivered += 1
            if self.store.is_acked(delivery.source, delivery.sequence):
                result.acked += 1

    @staticmethod
    def _is_cancel(error):
        code = getattr(error, 'code', None)
        return callable(code) and code() == grpc.StatusCode.CANCELLED

    def _handle_envelope(self, envelope_bytes, result):
        envelope = self.envelope_class()
        envelope.ParseFromString(envelope_bytes)
        source = envelope.source
        sequence = str(envelope.sequence)
        descriptor_id = envelope.descriptor_id
        type_url = envelope.payload.type_url
        payload_bytes = envelope.payload.value

        result.received += 1
        if descriptor_id not in result.descriptor_ids:
            result.descriptor_ids.append(descriptor_id)

        envelope_digest = self.store.put_object(envelope_bytes)
        payload_digest = self.store.put_object(payload_bytes)

        existing = self.store.find(source, sequence)
        replay = existing is not None
        decoded = None
        status = 'pending_descriptor'
        decode_error = None
        bundle = self.registry.get(descriptor_id)
        fingerprint = bundle.fingerprint if bundle is not None else None

        if bundle is not None:
            try:
                _, decoded = decode_any_payload(bundle, type_url, payload_bytes)
                status = 'decoded'
            except UnknownTypeError as error:
                decode_error = str(error)

        if status == 'pending_descriptor':
            result.pending_descriptor += 1

        record = {
            'source': source,
            'sequence': sequence,
            'descriptor_id': descriptor_id,
            'descriptor_fingerprint': fingerprint,
            'type_url': type_url,
            'type_full_name': type_url_full_name(type_url),
            'envelope_sha256': envelope_digest,
            'payload_sha256': payload_digest,
            'payload_bytes': len(payload_bytes),
            'status': status,
            'decode_error': decode_error,
            'decoded': decoded,
            'received_at': time.time(),
        }
        saved, existed = self.store.upsert_record(record)
        if existed:
            # 重播以已经持久化的记录为准：它可能在两次接收之间被重新解释过，
            # 本次内存里的临时解码不能覆盖归档状态与描述符留痕。
            replay = True
            decoded = saved.get('decoded')
            status = saved.get('status', status)

        return Delivery(
            source=source,
            sequence=sequence,
            descriptor_id=saved['descriptor_id'],
            type_url=type_url,
            envelope_sha256=envelope_digest,
            payload_sha256=payload_digest,
            status=status,
            decoded=decoded,
            record=saved,
            replay=replay,
        )
