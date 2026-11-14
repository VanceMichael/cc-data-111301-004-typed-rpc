import hashlib
import tempfile
import unittest
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from typed_rpc.descriptors import (
    DescriptorRegistry,
    MissingDependencyError,
    UnknownTypeError,
    parse_file_descriptor_set,
    type_url_full_name,
)


def _file(name, package, message_name, *, dependency=None, fields=()):
    file_proto = descriptor_pb2.FileDescriptorProto(
        name=name, package=package, syntax='proto3')
    message = file_proto.message_type.add(name=message_name)
    for field in fields:
        message.field.add(**field)
    if dependency:
        file_proto.dependency.append(dependency)
    return file_proto


def _set_bytes(file_proto):
    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.add().CopyFrom(file_proto)
    return fds.SerializeToString()


class DescriptorRegistryTests(unittest.TestCase):
    def test_fixture_bundles_have_distinct_fingerprints(self):
        registry = DescriptorRegistry()
        v1, added1 = registry.register('v1', Path('fixtures/v1.pb').read_bytes())
        v2, added2 = registry.register('v2', Path('fixtures/v2.pb').read_bytes())
        self.assertTrue(added1 and added2)
        self.assertNotEqual(v1.fingerprint, v2.fingerprint)
        self.assertEqual(hashlib.sha256(Path('fixtures/v1.pb').read_bytes())
                         .hexdigest(), v1.fingerprint)

    def test_same_full_name_keeps_distinct_definitions(self):
        registry = DescriptorRegistry()
        registry.register('v1', Path('fixtures/v1.pb').read_bytes())
        registry.register('v2', Path('fixtures/v2.pb').read_bytes())
        v1_class = registry.resolve('v1', 'type.example/plugin.Reading')[1]
        v2_class = registry.resolve('v2', 'type.example/plugin.Reading')[1]
        self.assertNotIn('device', v1_class.DESCRIPTOR.fields_by_name)
        self.assertIn('device', v2_class.DESCRIPTOR.fields_by_name)
        # 类型缓存按 bundle 隔离，不同池的同名消息类不是同一个对象。
        self.assertIsNot(v1_class, v2_class)
        self.assertIs(registry.resolve('v1', 'type.example/plugin.Reading')[1],
                      v1_class)

    def test_alternating_update_keeps_old_bundle_readable(self):
        registry = DescriptorRegistry()
        first, _ = registry.register('edge', Path('fixtures/v1.pb').read_bytes())
        second, changed = registry.register('edge', Path('fixtures/v2.pb').read_bytes())
        self.assertTrue(changed)
        self.assertIsNot(first, second)
        # 旧消息仍按旧定义回读：v2 多出的字段对旧池不存在。
        old_class = first.message_class('plugin.Reading')
        self.assertNotIn('device', old_class.DESCRIPTOR.fields_by_name)
        # 重新登记相同内容是幂等的。
        again, changed_again = registry.register(
            'edge', Path('fixtures/v2.pb').read_bytes())
        self.assertFalse(changed_again)
        self.assertIs(again, second)

    def test_dependencies_may_arrive_out_of_order(self):
        leaf = _file('leaf.proto', 'demo', 'Leaf', fields=[
            dict(name='value', number=1, label=1,
                 type=descriptor_pb2.FieldDescriptorProto.TYPE_INT64)])
        root = _file('root.proto', 'demo', 'Root', dependency='leaf.proto',
                     fields=[dict(name='leaf', number=1, label=1,
                                  type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
                                  type_name='.demo.Leaf')])
        shuffled = descriptor_pb2.FileDescriptorSet()
        shuffled.file.add().CopyFrom(root)   # 依赖者先出现
        shuffled.file.add().CopyFrom(leaf)   # 被依赖者后出现
        registry = DescriptorRegistry()
        bundle, _ = registry.register('shuffled', shuffled.SerializeToString())
        message_class = bundle.message_class('demo.Root')
        message = message_class()
        message.leaf.value = 7
        raw = message.SerializeToString()
        message_class().ParseFromString(raw)

    def test_missing_dependency_is_surface_not_guessed(self):
        root = _file('root.proto', 'demo', 'Root', dependency='missing.proto')
        registry = DescriptorRegistry()
        with self.assertRaises(MissingDependencyError):
            registry.register('broken', _set_bytes(root))

    def test_well_known_dependency_is_bootstrapped(self):
        file_proto = descriptor_pb2.FileDescriptorProto(
            name='uses_any.proto', package='demo', syntax='proto3')
        file_proto.dependency.append('google/protobuf/any.proto')
        message = file_proto.message_type.add(name='Envelope')
        message.field.add(name='payload', number=1, label=1,
                          type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
                          type_name='.google.protobuf.Any')
        registry = DescriptorRegistry()
        bundle, _ = registry.register('any-user', _set_bytes(file_proto))
        self.assertTrue(bundle.knows_type('demo.Envelope'))

    def test_unknown_type_url_is_an_explicit_error(self):
        registry = DescriptorRegistry()
        registry.register('v1', Path('fixtures/v1.pb').read_bytes())
        with self.assertRaises(UnknownTypeError):
            registry.resolve('v1', 'type.example/plugin.Missing')
        self.assertEqual(type_url_full_name('type.example/a.b.C'), 'a.b.C')
        self.assertEqual(type_url_full_name('a.b.C'), 'a.b.C')

    def test_save_and_load_roundtrip(self):
        registry = DescriptorRegistry()
        registry.register('v1', Path('fixtures/v1.pb').read_bytes())
        registry.register('v2', Path('fixtures/v2.pb').read_bytes())
        with tempfile.TemporaryDirectory() as directory:
            index = registry.save(directory)
            self.assertEqual({entry['descriptor_id'] for entry in index},
                             {'v1', 'v2'})
            rebuilt = DescriptorRegistry()
            rebuilt.load(directory, index)
            self.assertEqual(
                rebuilt.resolve('v2', 'type.example/plugin.Reading')[1]
                .DESCRIPTOR.fields_by_name['device'].name, 'device')

    def test_invalid_descriptor_set_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_file_descriptor_set(b'not a descriptor set')
        empty = descriptor_pb2.FileDescriptorSet().SerializeToString()
        with self.assertRaises(ValueError):
            parse_file_descriptor_set(empty)

    def test_pools_are_independent_instances(self):
        registry = DescriptorRegistry()
        registry.register('v1', Path('fixtures/v1.pb').read_bytes())
        registry.register('v2', Path('fixtures/v2.pb').read_bytes())
        pools = {bundle.pool for bundle in
                 (registry.get('v1'), registry.get('v2'))}
        self.assertEqual(len(pools), 2)
        # 全局默认池没有被插件类型污染。
        with self.assertRaises(KeyError):
            descriptor_pool.Default().FindMessageTypeByName('plugin.Reading')
        # message_factory 的默认符号库同样不含插件类型。
        with self.assertRaises(KeyError):
            message_factory.GetMessageClass(
                descriptor_pool.Default().FindMessageTypeByName('plugin.Reading'))


if __name__ == '__main__':
    unittest.main()
