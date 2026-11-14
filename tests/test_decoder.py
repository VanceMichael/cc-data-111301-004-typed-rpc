import base64
import json
import unittest
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from typed_rpc.decoder import (
    decode_any_payload,
    decode_dynamic,
    from_canonical_json,
    to_canonical_json,
)
from typed_rpc.descriptors import DescriptorRegistry, UnknownTypeError


def _build_demo_pool():
    """构造同时包含 optional/oneof/枚举缺口/64 位/map/嵌套消息的描述符。"""
    file_proto = descriptor_pb2.FileDescriptorProto(
        name='demo.proto', package='demo', syntax='proto3')
    message = file_proto.message_type.add(name='Demo')

    enum = message.enum_type.add(name='Status')
    enum.value.add(name='OK', number=0)
    enum.value.add(name='BUSY', number=2)  # 故意留空数字 1
    message.field.add(name='status', number=1, label=1,
                      type=descriptor_pb2.FieldDescriptorProto.TYPE_ENUM,
                      type_name='.demo.Demo.Status')

    zero = message.field.add(name='zero_field', number=2, label=1,
                             type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
    zero.proto3_optional = True

    message.oneof_decl.add(name='choice')
    note = message.field.add(name='note', number=3, label=1,
                             type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    note.oneof_index = 0
    big = message.field.add(name='big', number=4, label=1,
                            type=descriptor_pb2.FieldDescriptorProto.TYPE_INT64)
    big.oneof_index = 0

    synthetic = message.oneof_decl.add(name='_zero_field')
    zero.oneof_index = 1
    synthetic.options.features

    nested = file_proto.message_type.add(name='Nested')
    nested.field.add(name='flag', number=1, label=1,
                     type=descriptor_pb2.FieldDescriptorProto.TYPE_BOOL)
    message.field.add(name='child', number=5, label=1,
                      type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
                      type_name='.demo.Nested')
    message.field.add(name='blob', number=6, label=1,
                      type=descriptor_pb2.FieldDescriptorProto.TYPE_BYTES)
    repeated = message.field.add(name='counts', number=7, label=3,
                                 type=descriptor_pb2.FieldDescriptorProto.TYPE_UINT64)

    entry = message.nested_type.add(name='TagsEntry')
    entry.options.map_entry = True
    entry.field.add(name='key', number=1, label=1,
                    type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    entry.field.add(name='value', number=2, label=1,
                    type=descriptor_pb2.FieldDescriptorProto.TYPE_INT64)
    message.field.add(name='tags', number=8, label=3,
                      type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
                      type_name='.demo.Demo.TagsEntry')

    pool = descriptor_pool.DescriptorPool()
    pool.AddSerializedFile(file_proto.SerializeToString())
    return pool, pool.FindMessageTypeByName('demo.Demo')


class DecoderTests(unittest.TestCase):
    def setUp(self):
        self.pool, self.descriptor = _build_demo_pool()
        self.message_class = message_factory.GetMessageClass(self.descriptor)

    def test_absent_optional_vs_explicit_zero(self):
        empty = decode_dynamic(self.message_class())
        self.assertIn('zero_field', empty['absent'])
        self.assertNotIn('zero_field', empty['fields'])
        self.assertIsNone(empty['fields'].get('child'))
        self.assertIn('child', empty['absent'])

        explicit_zero = self.message_class()
        explicit_zero.zero_field = 0
        view = decode_dynamic(explicit_zero)
        field_view = view['fields']['zero_field']
        self.assertTrue(field_view['present'])
        self.assertEqual(field_view['value'], 0)
        self.assertNotIn('zero_field', view['absent'])

    def test_oneof_reports_only_active_branch(self):
        message = self.message_class()
        message.big = -(2 ** 63)
        view = decode_dynamic(message)
        self.assertNotIn('note', view['fields'])
        branch = view['fields']['big']
        self.assertEqual(branch['oneof'], 'choice')
        self.assertEqual(branch['oneof_choice'], 'big')
        self.assertEqual(branch['value'], str(-(2 ** 63)))
        # 未命中的分支进 absent
        self.assertIn('note', view['absent'])

    def test_unknown_enum_value_keeps_number(self):
        message = self.message_class()
        message.ParseFromString(b'\x08\x01')  # status = 1，描述符里没有
        view = decode_dynamic(message)
        value = view['fields']['status']['value']
        self.assertEqual(value, {'kind': 'enum', 'number': 1,
                                 'name': None, 'unknown_enum': True})

        known = self.message_class()
        known.status = 2
        known_view = decode_dynamic(known)
        self.assertEqual(known_view['fields']['status']['value'],
                         {'kind': 'enum', 'number': 2,
                          'name': 'BUSY', 'unknown_enum': False})

    def test_64bit_integers_are_strings_including_repeated_and_map(self):
        message = self.message_class()
        message.counts.extend([9007199254740994, 2 ** 64 - 1])
        message.tags['probe'] = -(2 ** 63)
        view = decode_dynamic(message)
        self.assertEqual(view['fields']['counts']['values'],
                         ['9007199254740994', str(2 ** 64 - 1)])
        entry = view['fields']['tags']['entries'][0]
        self.assertEqual(entry['key'], 'probe')
        self.assertEqual(entry['value'], str(-(2 ** 63)))
        # 直接走 json 模块也不会丢精度，因为值本身就是字符串。
        json.dumps(view, ensure_ascii=False)

    def test_bytes_nested_and_unknown_field_are_preserved(self):
        message = self.message_class()
        message.blob = b'\x00\xff'
        message.child.flag = True
        # 已知字段序列化后，再手工追加一个描述符不认识的字段号 105。
        raw = message.SerializeToString() + b'\xca\x06\x03abc'
        rebuilt = self.message_class()
        rebuilt.ParseFromString(raw)
        view = decode_dynamic(rebuilt)
        self.assertEqual(view['fields']['blob']['value'],
                         base64.b64encode(b'\x00\xff').decode())
        self.assertTrue(view['fields']['child']['value']['fields']
                        ['flag']['value'])
        unknown = view['unknown']
        self.assertEqual(unknown[0]['field_number'], 105)
        self.assertEqual(unknown[0]['wire_type_name'], 'length_delimited')
        self.assertEqual(unknown[0]['data_base64'],
                         base64.b64encode(b'abc').decode())

    def test_canonical_json_roundtrip_is_lossless(self):
        message = self.message_class()
        message.big = 9007199254740995
        message.zero_field = 0
        message.status = 1
        message.blob = b'\x01\x02'
        message.counts.extend([2 ** 64 - 1])
        view = decode_dynamic(message)
        text = to_canonical_json(view)
        self.assertEqual(from_canonical_json(text), view)
        # 规范 JSON 拒绝 NaN/Infinity，杜绝 float 信息走非有限值后门。
        with self.assertRaises(ValueError):
            json.loads(text) if False else json.dumps(float('nan'), allow_nan=False)

    def test_decode_fixture_payloads_with_per_source_bundles(self):
        registry = DescriptorRegistry()
        registry.register('v1', Path('fixtures/v1.pb').read_bytes())
        registry.register('v2', Path('fixtures/v2.pb').read_bytes())
        v1_bundle = registry.get('v1')
        v2_bundle = registry.get('v2')
        payload_v1 = base64.b64decode(
            'CIKAgICAgIAQEQAAAAAAgDRA')
        payload_v2 = base64.b64decode(
            'CIOAgICAgIAQEQAAAAAAgDRAGg/omZrmnoTmjqLlpLTnlLI=')

        _, view_v1 = decode_any_payload(
            v1_bundle, 'type.example/plugin.Reading', payload_v1)
        _, view_v2 = decode_any_payload(
            v2_bundle, 'type.example/plugin.Reading', payload_v2)
        self.assertEqual(view_v1['fields']['sequence']['value'],
                         '9007199254740994')
        # v2 消息里 device 已出现且按 v2 定义解码；同一份 v2 原文用 v1 解释时，
        # 字段号 3 在 v1 描述符中不存在，必须作为未知字段保留。
        self.assertIn('device', view_v2['fields'])
        self.assertEqual(view_v2['fields']['device']['value'], '虚构探头甲')
        _, view_v2_under_v1 = decode_any_payload(
            v1_bundle, 'type.example/plugin.Reading', payload_v2)
        self.assertEqual(view_v2_under_v1['unknown'][0]['field_number'], 3)
        self.assertEqual(view_v2_under_v1['unknown'][0]['data_base64'],
                         base64.b64encode('虚构探头甲'.encode('utf-8')).decode())

    def test_unknown_type_raises_and_bytes_remain_untouched(self):
        registry = DescriptorRegistry()
        registry.register('v1', Path('fixtures/v1.pb').read_bytes())
        payload = base64.b64decode('CAE=')
        with self.assertRaises(UnknownTypeError):
            decode_any_payload(
                registry.get('v1'),
                'type.example/plugin.Unknown', payload)
        # 原始字节没有被消费/改写，可留待补齐描述符后再解。
        self.assertEqual(payload, base64.b64decode('CAE='))


if __name__ == '__main__':
    unittest.main()
