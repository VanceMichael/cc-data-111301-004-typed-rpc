import unittest
import tempfile
from pathlib import Path

from typed_rpc.store import ReceiveStore


def _record(sequence, envelope=b'env', payload=b'pay', status='decoded',
            descriptor_id='v1', fingerprint='fp'):
    return {
        'source': 'lab-a',
        'sequence': str(sequence),
        'descriptor_id': descriptor_id,
        'descriptor_fingerprint': fingerprint,
        'type_url': 'type.example/plugin.Reading',
        'type_full_name': 'plugin.Reading',
        'envelope_sha256': 'env-' + str(sequence),
        'payload_sha256': 'pay-' + str(sequence),
        'payload_bytes': len(payload),
        'status': status,
        'decode_error': None,
        'decoded': {'sequence': str(sequence)},
    }


class ReceiveStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = ReceiveStore(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def test_object_archive_is_content_addressed_and_roundtrips(self):
        digest = self.store.put_object(b'hello')
        self.assertEqual(self.store.get_object(digest), b'hello')
        again = self.store.put_object(b'hello')
        self.assertEqual(again, digest)
        self.assertEqual(len(list((Path(self.directory.name) / 'objects').iterdir())), 1)

    def test_upsert_is_idempotent_and_rejects_conflicting_sequence(self):
        record, existed = self.store.upsert_record(_record(1))
        self.assertFalse(existed)
        duplicate, existed_again = self.store.upsert_record(_record(1))
        self.assertTrue(existed_again)
        self.assertEqual(duplicate, record)

        conflict = _record(1)
        conflict['envelope_sha256'] = 'different'
        with self.assertRaises(ValueError):
            self.store.upsert_record(conflict)

    def test_ack_persistence_distinguishes_handled_and_confirmed(self):
        self.store.upsert_record(_record(1))
        self.assertFalse(self.store.is_acked('lab-a', '1'))
        self.assertTrue(self.store.ack('lab-a', '1', downstream='drill'))
        self.assertTrue(self.store.is_acked('lab-a', '1'))
        # 重复确认不产生第二条日志。
        self.assertFalse(self.store.ack('lab-a', '1', downstream='drill'))
        self.assertEqual(len(self.store.list_acks()), 1)
        # 另一条已交给下游但还没确认的记录保持未确认。
        self.store.upsert_record(_record(2, status='pending_descriptor'))
        self.assertFalse(self.store.is_acked('lab-a', '2'))

    def test_reopen_recovers_records_and_acks(self):
        self.store.upsert_record(_record(1))
        self.store.ack('lab-a', '1')
        reopened = ReceiveStore(self.directory.name)
        self.assertEqual(reopened.find('lab-a', '1')['envelope_sha256'], 'env-1')
        self.assertTrue(reopened.is_acked('lab-a', '1'))

    def test_update_view_appends_history_and_changes_latest(self):
        self.store.upsert_record(_record(3, status='pending_descriptor',
                                         fingerprint=None))
        updated = self.store.update_view(
            'lab-a', '3', 'decoded', {'fields': {}}, 'unknown', 'new-fp')
        self.assertEqual(updated['descriptor_fingerprint'], 'new-fp')
        self.assertEqual(self.store.find('lab-a', '3')['status'], 'decoded')
        # jsonl 保留历史行，可追溯描述符替换前后的解释。
        lines = (Path(self.directory.name) / 'records.jsonl').read_text(
            encoding='utf-8').splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn('pending_descriptor', lines[0])
        self.assertIn('new-fp', lines[1])

    def test_run_history_records_end_reason(self):
        self.store.append_run({'ended': 'completed', 'status_code': 'OK'})
        self.store.append_run({'ended': 'rpc_error',
                               'status_code': 'UNAVAILABLE'})
        runs = self.store.list_runs()
        self.assertEqual([run['ended'] for run in runs],
                         ['completed', 'rpc_error'])


if __name__ == '__main__':
    unittest.main()
