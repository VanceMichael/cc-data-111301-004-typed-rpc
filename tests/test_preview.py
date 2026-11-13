from pathlib import Path
import tempfile
import unittest

from typed_rpc.core import inspect_file


class PreviewTests(unittest.TestCase):
    def test_source_preview(self):
        result = inspect_file(Path('fixtures/v1.pb'))
        self.assertEqual(result['files'][0]['package'], 'plugin')
        self.assertEqual(result['files'][0]['messages'], ['Reading'])

    def test_invalid_source_is_not_empty_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'invalid'
            path.write_bytes(b'not a valid source')
            with self.assertRaises(Exception):
                inspect_file(path)

    def test_missing_source_is_not_empty_success(self):
        with self.assertRaises(FileNotFoundError):
            inspect_file(Path('fixtures/does-not-exist'))
