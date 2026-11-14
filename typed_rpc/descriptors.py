"""按来源描述符建立的动态类型注册中心。

每条资料流可能携带多个来源、每个来源又可能交替发布多套描述符。这里刻意不
使用按消息全名共享的全局类型缓存：每收到一份 FileDescriptorSet 就建立独立的
DescriptorPool，同名消息在不同来源/不同版本里可以拥有不同定义。

依赖文件允许乱序出现：注册时先做拓扑排序，缺失的 google/protobuf/* 依赖从
运行时自带的已知类型描述符补齐；仍然缺失的依赖会在解释消息时显式报错，而
不会静默落到错误的定义上。
"""

import hashlib
from pathlib import Path

from google.protobuf import (
    any_pb2,
    api_pb2,
    descriptor_pb2,
    descriptor_pool,
    duration_pb2,
    empty_pb2,
    field_mask_pb2,
    source_context_pb2,
    struct_pb2,
    timestamp_pb2,
    type_pb2,
    wrappers_pb2,
)
from google.protobuf import message_factory

# 导入这些模块会把 google/protobuf/*.proto 注册进默认池，作为依赖引导来源。
_WELL_KNOWN_IMPORTS = (
    any_pb2,
    api_pb2,
    duration_pb2,
    empty_pb2,
    field_mask_pb2,
    source_context_pb2,
    struct_pb2,
    timestamp_pb2,
    type_pb2,
    wrappers_pb2,
)


def well_known_file(name):
    """返回运行时自带已知类型文件的序列化描述符，找不到时返回 None。"""
    try:
        file_descriptor = descriptor_pool.Default().FindFileByName(name)
    except KeyError:
        return None
    return file_descriptor.serialized_pb


class MissingDependencyError(LookupError):
    """描述符集合引用了当前还无法补齐的依赖文件。"""


class UnknownTypeError(LookupError):
    """来源描述符里找不到 Any 类型 URL 指向的消息。"""


class DescriptorBundle:
    """一次 Describe 返回的整套描述符及其独立类型池。"""

    def __init__(self, descriptor_id, raw, files):
        self.descriptor_id = descriptor_id
        self.raw = bytes(raw)
        self.files = list(files)
        self.fingerprint = hashlib.sha256(self.raw).hexdigest()
        self.pool = self._build_pool(self.files)
        self._message_cache = {}

    @staticmethod
    def _build_pool(files):
        pool = descriptor_pool.DescriptorPool()
        by_name = {file_proto.name: file_proto for file_proto in files}

        def load(file_name, seen):
            if file_name in seen:
                return
            if file_name not in by_name:
                well_known = well_known_file(file_name)
                if well_known is None:
                    raise MissingDependencyError(
                        '描述符依赖尚未到达：' + file_name)
                # 已知类型文件的依赖同样需要先入池。
                dependency_proto = descriptor_pb2.FileDescriptorProto()
                dependency_proto.ParseFromString(well_known)
                by_name[file_name] = dependency_proto
            file_proto = by_name[file_name]
            seen.add(file_name)
            for dependency in file_proto.dependency:
                load(dependency, seen)
            pool.AddSerializedFile(file_proto.SerializeToString())

        visited = set()
        for file_proto in files:
            load(file_proto.name, visited)
        return pool

    def message_class(self, full_name):
        cached = self._message_cache.get(full_name)
        if cached is not None:
            return cached
        try:
            descriptor = self.pool.FindMessageTypeByName(full_name)
        except KeyError as error:
            raise UnknownTypeError(
                '描述符 %s 中没有类型 %s' % (self.descriptor_id, full_name)
            ) from error
        message_class = message_factory.GetMessageClass(descriptor)
        self._message_cache[full_name] = message_class
        return message_class

    def knows_type(self, full_name):
        try:
            self.message_class(full_name)
        except UnknownTypeError:
            return False
        return True

    def type_names(self):
        names = []
        for file_proto in self.files:
            prefix = file_proto.package + '.' if file_proto.package else ''
            for message in file_proto.message_type:
                names.append(prefix + message.name)
        return sorted(names)


def parse_file_descriptor_set(raw):
    bundle = descriptor_pb2.FileDescriptorSet()
    try:
        bundle.ParseFromString(bytes(raw))
    except Exception as error:
        raise ValueError('FileDescriptorSet 无法解析') from error
    if not bundle.file:
        raise ValueError('描述符集合为空')
    return list(bundle.file)


def type_url_full_name(type_url):
    """从 type.googleapis.com/pkg.Name 形式的 URL 取消息全名。"""
    if '/' not in type_url:
        return type_url
    return type_url.rsplit('/', 1)[1]


class DescriptorRegistry:
    """以 (descriptor_id) 为键的描述符集合登记处。

    同一个 descriptor_id 可以交替更新：每次注册都建立新的 bundle 与独立类型
    池，旧 bundle 仍然保留，已经归档的旧消息可以继续按原定义回读。
    """

    def __init__(self):
        self._bundles = {}

    def register(self, descriptor_id, raw):
        files = parse_file_descriptor_set(raw)
        bundle = DescriptorBundle(descriptor_id, raw, files)
        existing = self._bundles.get(descriptor_id)
        if existing is not None and existing.fingerprint == bundle.fingerprint:
            return existing, False
        self._bundles[descriptor_id] = bundle
        return bundle, True

    def get(self, descriptor_id):
        return self._bundles.get(descriptor_id)

    def require(self, descriptor_id):
        bundle = self._bundles.get(descriptor_id)
        if bundle is None:
            raise LookupError('来源描述符尚未登记：' + descriptor_id)
        return bundle

    def resolve(self, descriptor_id, type_url):
        bundle = self.require(descriptor_id)
        return bundle, bundle.message_class(type_url_full_name(type_url))

    def descriptor_ids(self):
        return sorted(self._bundles)

    def save(self, directory):
        """把每份描述符原文按内容指纹落盘，便于重启后重新登记。"""
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        written = []
        for descriptor_id, bundle in self._bundles.items():
            target = root / (bundle.fingerprint + '.pb')
            if not target.exists():
                target.write_bytes(bundle.raw)
            written.append({
                'descriptor_id': descriptor_id,
                'fingerprint': bundle.fingerprint,
                'object': str(target),
                'types': bundle.type_names(),
            })
        return written

    def load(self, directory, index):
        """按 save 产出的索引清单从归档目录恢复描述符。"""
        root = Path(directory)
        restored = []
        for entry in index:
            target = root / (entry['fingerprint'] + '.pb')
            bundle, _ = self.register(entry['descriptor_id'], target.read_bytes())
            if bundle.fingerprint != entry['fingerprint']:
                raise ValueError('描述符归档与指纹不一致：' + entry['fingerprint'])
            restored.append(bundle)
        return restored
