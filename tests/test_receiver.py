import json
import tempfile
import threading
import unittest
from pathlib import Path

import grpc

from typed_rpc.descriptors import DescriptorRegistry
from typed_rpc.receiver import Receiver
from typed_rpc.reinterpret import reinterpret_pending, reinterpret_record
from typed_rpc.rpc_source import rpc_source
from typed_rpc.store import ReceiveStore


class ReceiverIntegrationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store_dir = self._tmp.name
        self.registry = DescriptorRegistry()
        self.store = ReceiveStore(self.store_dir)

    def tearDown(self):
        self._tmp.cleanup()

    def _receiver(self, channel, **kwargs):
        return Receiver(channel, self.registry, self.store, **kwargs)

    def test_full_stream_decodes_and_preserves_unknown_any(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(channel)
            receiver.preload_descriptors(['v1', 'v2'])
            acks = []

            def handler(delivery):
                if delivery.status == 'decoded':
                    delivery.ack(self.store, downstream='drill')
                acks.append((delivery.source, delivery.sequence,
                             delivery.status))

            result = receiver.run(b'{}', handler=handler)
            channel.close()

        self.assertEqual(result.ended, 'completed')
        self.assertEqual(result.status_code, grpc.StatusCode.OK)
        self.assertEqual(result.received, 3)
        self.assertEqual(result.pending_descriptor, 1)
        records = self.store.list_records()
        self.assertEqual(len(records), 3)

        unknown_record = self.store.find('lab-b', '3')
        self.assertEqual(unknown_record['status'], 'pending_descriptor')
        # 未知 Any 的原字节与 type_url 完整保留，可按摘要取回。
        self.assertEqual(self.store.get_object(
            unknown_record['payload_sha256']), b'\x08\x01')
        self.assertEqual(unknown_record['type_url'],
                         'type.example/plugin.Unknown')
        # 未补齐描述符的消息没有确认。
        self.assertFalse(self.store.is_acked('lab-b', '3'))
        self.assertTrue(self.store.is_acked('lab-a', '1'))

        first = self.store.find('lab-a', '1')
        # 记录能查到使用了哪组描述符（id 与内容指纹）。
        self.assertEqual(first['descriptor_id'], 'v1')
        self.assertEqual(first['descriptor_fingerprint'],
                         self.registry.get('v1').fingerprint)
        self.assertEqual(first['decoded']['fields']['sequence']['value'],
                         '9007199254740994')

    def test_non_ok_server_status_is_not_normal_completion(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(channel)
            receiver.preload_descriptors(['v1', 'v2'])
            result = receiver.run(b'{"fail_end":true}')
            channel.close()
        self.assertEqual(result.ended, 'rpc_error')
        self.assertEqual(result.status_code, grpc.StatusCode.UNAVAILABLE)
        self.assertNotEqual(result.ended, 'completed')
        self.assertEqual(result.received, 3)
        run_row = self.store.list_runs()[-1]
        self.assertEqual(run_row['status_code'], 'UNAVAILABLE')

    def test_client_cancellation_is_recorded_as_cancelled(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(channel)
            receiver.preload_descriptors(['v1', 'v2'])
            first = threading.Event()

            def handler(delivery):
                first.set()

            thread = threading.Thread(
                target=receiver.run,
                kwargs={'request': b'{"hang_after":1}', 'handler': handler})
            thread.start()
            self.assertTrue(first.wait(timeout=5))
            # 源在第一条之后挂起，此时取消必然发生在流进行中。
            receiver.cancel()
            thread.join(timeout=5)
            channel.close()
        self.assertFalse(thread.is_alive())
        run_row = self.store.list_runs()[-1]
        self.assertEqual(run_row['ended'], 'cancelled')
        self.assertEqual(run_row['status_code'], 'CANCELLED')

    def test_bounded_queue_applies_backpressure(self):
        # 队列容量 1、入队超时 0.2s：下游在第一条消息上挂住后，读取线程很快
        # 因队列满而主动以 RESOURCE_EXHAUSTED 结束，不会无界堆积或假装完成。
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(
                channel, queue_capacity=1, enqueue_timeout=0.2)
            stalled = threading.Event()
            release = threading.Event()

            def handler(delivery):
                if not stalled.is_set():
                    stalled.set()
                    release.wait(timeout=5)

            thread = threading.Thread(
                target=receiver.run,
                kwargs={'request': b'{}', 'handler': handler})
            thread.start()
            self.assertTrue(stalled.wait(timeout=5))
            # 等待读取线程走过入队超时并主动取消流。
            thread.join(timeout=3)
            release.set()
            thread.join(timeout=3)
            channel.close()

        self.assertFalse(thread.is_alive())
        run_row = self.store.list_runs()[-1]
        self.assertEqual(run_row['ended'], 'rpc_error')
        self.assertEqual(run_row['status_code'], 'RESOURCE_EXHAUSTED')
        self.assertNotEqual(run_row['ended'], 'completed')

    def test_downstream_failure_is_an_error_not_completion(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(channel)
            receiver.preload_descriptors(['v1', 'v2'])

            def broken_handler(delivery):
                raise RuntimeError('下游宕机')

            with self.assertRaises(RuntimeError):
                receiver.run(b'{}', handler=broken_handler)
            channel.close()
        run_row = self.store.list_runs()[-1]
        self.assertEqual(run_row['ended'], 'error')
        self.assertNotIn(run_row['status_code'], ('OK', None))

    def test_replay_redelivers_unacked_and_skips_acked(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(channel)
            receiver.preload_descriptors(['v1', 'v2'])

            # 第一轮：只确认序号 1，序号 2 处理了但确认尚未持久化。
            first_pass = []

            def first_handler(delivery):
                first_pass.append(delivery.sequence)
                if delivery.sequence == '1':
                    delivery.ack(self.store)

            receiver.run(b'{}', handler=first_handler)
            self.assertEqual(self.store.list_runs()[-1]['ended'], 'completed')

            # 第二轮：来源重播全部消息。
            deliveries = []

            def second_handler(delivery):
                deliveries.append((delivery.sequence, delivery.replay))

            second = self._receiver(channel)
            result = second.run(b'{}', handler=second_handler)
            channel.close()

        self.assertEqual(result.ended, 'completed')
        replay_sequences = {seq for seq, replay in deliveries if replay}
        # 已确认的 1 不再投递；2 与未确认的 3 重新投递。
        self.assertNotIn('1', replay_sequences)
        self.assertIn('2', replay_sequences)
        self.assertIn('3', replay_sequences)
        self.assertEqual(result.redelivered, 2)
        # 重播没有产生重复记录。
        self.assertEqual(len(self.store.list_records()), 3)

    def test_supplemental_descriptor_reinterprets_unknown_message(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(channel)
            # 故意只预取 v1/v2，lab-b 的插件类型保持未知。
            receiver.preload_descriptors(['v1', 'v2'])
            receiver.run(b'{}')
            channel.close()

        unknown_record = self.store.find('lab-b', '3')
        self.assertEqual(unknown_record['status'], 'pending_descriptor')

        # 补交原先未知的插件类型描述符后重新解释。
        self.registry.register(
            'unknown', Path('fixtures/unknown.pb').read_bytes())
        updated, pending = reinterpret_pending(self.store, self.registry)
        self.assertEqual(pending, [])
        self.assertEqual(len(updated), 1)
        refreshed = updated[0]
        self.assertEqual(refreshed['descriptor_id'], 'unknown')
        self.assertEqual(refreshed['descriptor_fingerprint'],
                         self.registry.get('unknown').fingerprint)

        decoded = refreshed['decoded']
        # 枚举未知值：线编码 1 在描述符中没有命名值。
        state = decoded['fields']['state']['value']
        self.assertTrue(state['unknown_enum'])
        self.assertEqual(state['number'], 1)
        # 未出现的 proto3 optional 字段进入 absent。
        self.assertIn('note', decoded['absent'])
        # 未命中的 oneof 分支同样缺席。
        self.assertIn('label', decoded['absent'])
        self.assertIn('weight', decoded['absent'])

        # 重新解释是从归档原字节重建的，与来源是否在线无关。
        self.assertEqual(self.store.get_object(refreshed['payload_sha256']),
                         b'\x08\x01')

    def test_alternating_descriptor_sets_keep_old_messages_readable(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(channel)
            receiver.preload_descriptors(['v1', 'v2'])
            receiver.run(b'{}')
            channel.close()

        v1_record = self.store.find('lab-a', '1')
        v2_record = self.store.find('lab-a', '2')
        # 两条同名消息各自绑定不同描述符指纹。
        self.assertNotEqual(v1_record['descriptor_fingerprint'],
                            v2_record['descriptor_fingerprint'])

        # 来源把 v1 交替更新成 v2 内容后，旧消息仍按原指纹的定义回读。
        self.registry.register('v1', Path('fixtures/v2.pb').read_bytes())
        rebuilt_old = reinterpret_record(
            self.store, self.registry, v1_record)
        # v1 已被替换，重新解释会刷新指纹；但归档原字节未变，且历史行可追溯。
        self.assertEqual(rebuilt_old['envelope_sha256'],
                         v1_record['envelope_sha256'])
        history = (Path(self.store_dir) / 'records.jsonl').read_text(
            encoding='utf-8').splitlines()
        fingerprints = {json.loads(line)['descriptor_fingerprint']
                        for line in history
                        if json.loads(line)['sequence'] == '1'}
        self.assertIn(v1_record['descriptor_fingerprint'], fingerprints)

    def test_replay_after_reinterpret_uses_archived_record(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            receiver = self._receiver(channel)
            receiver.preload_descriptors(['v1', 'v2'])
            receiver.run(b'{}')
            channel.close()

        # 补交描述符并重新解释第 3 条。
        self.registry.register(
            'unknown', Path('fixtures/unknown.pb').read_bytes())
        updated, _ = reinterpret_pending(self.store, self.registry)
        self.assertEqual(updated[0]['status'], 'decoded')

        # 再次接收（模拟来源重播）：投递对象必须携带归档后的 decoded 状态，
        # 而不是本次运行里因描述符未预取而得到的 pending_descriptor。
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            fresh_registry = DescriptorRegistry()  # 故意不含 unknown
            receiver = Receiver(channel, fresh_registry, self.store)
            receiver.preload_descriptors(['v1', 'v2'])
            seen = {}

            def handler(delivery):
                seen[delivery.sequence] = delivery

            result = receiver.run(b'{}', handler=handler)
            channel.close()

        self.assertEqual(result.ended, 'completed')
        redelivery = seen['3']
        self.assertTrue(redelivery.replay)
        self.assertEqual(redelivery.status, 'decoded')
        self.assertIsNotNone(redelivery.decoded)
        self.assertEqual(redelivery.record['descriptor_fingerprint'],
                         self.registry.get('unknown').fingerprint)

    def test_descriptor_loaded_late_during_run_still_archives_first(self):
        with rpc_source('fixtures') as address:
            channel = grpc.insecure_channel(address)
            # 不预取任何描述符：所有消息先按未知归档，再补描述符重新解释。
            receiver = self._receiver(channel)
            result = receiver.run(b'{}')
            channel.close()
        self.assertEqual(result.pending_descriptor, 3)
        for descriptor_id in ('v1', 'v2'):
            self.registry.register(
                descriptor_id,
                Path(f'fixtures/{descriptor_id}.pb').read_bytes())
        updated, pending = reinterpret_pending(self.store, self.registry)
        self.assertEqual(len(updated), 2)
        self.assertEqual(len(pending), 1)  # lab-b 仍缺插件描述符


if __name__ == '__main__':
    unittest.main()
