"""动态 Protobuf 消息的无损结构化解释。

业务交接曾经把消息转成普通字典再导出，插件新增字段后字段丢失，proto3 的
“未出现”和“显式零值”也被压成同一个值。这里直接基于描述符逐字段解释，输出
带形态标记的纯内置结构（dict/list/str/int/bool/float/None），并通过
to_canonical_json / from_canonical_json 保证一轮 JSON 往返不损失任何信息：

* 未出现的 optional 字段不会出现在 fields 里，只列入 absent；
* 显式零值照常出现，value 就是 0/0.0/''/False；
* oneof 只报告当前命中的分支及其归属；
* 枚举遇到描述符里没有的数字时保留数字并标记 unknown_enum；
* 64 位整数统一以十进制字符串承载，避免 JSON number 精度损失；
* 描述符不认识的字段号保留线编码细节，补齐描述符后可重新解释。
"""

import base64
import json

from google.protobuf import unknown_fields as unknown_fields_mod
from google.protobuf.descriptor import FieldDescriptor

from .descriptors import type_url_full_name

_CANONICAL_VERSION = 1

_SCALAR_KIND = {
    FieldDescriptor.TYPE_DOUBLE: 'double',
    FieldDescriptor.TYPE_FLOAT: 'float',
    FieldDescriptor.TYPE_INT64: 'int64',
    FieldDescriptor.TYPE_UINT64: 'uint64',
    FieldDescriptor.TYPE_INT32: 'int32',
    FieldDescriptor.TYPE_FIXED64: 'fixed64',
    FieldDescriptor.TYPE_FIXED32: 'fixed32',
    FieldDescriptor.TYPE_BOOL: 'bool',
    FieldDescriptor.TYPE_STRING: 'string',
    FieldDescriptor.TYPE_BYTES: 'bytes',
    FieldDescriptor.TYPE_UINT32: 'uint32',
    FieldDescriptor.TYPE_ENUM: 'enum',
    FieldDescriptor.TYPE_SFIXED32: 'sfixed32',
    FieldDescriptor.TYPE_SFIXED64: 'sfixed64',
    FieldDescriptor.TYPE_SINT32: 'sint32',
    FieldDescriptor.TYPE_SINT64: 'sint64',
}

_INTEGER64_TYPES = {
    FieldDescriptor.TYPE_INT64,
    FieldDescriptor.TYPE_UINT64,
    FieldDescriptor.TYPE_FIXED64,
    FieldDescriptor.TYPE_SFIXED64,
    FieldDescriptor.TYPE_SINT64,
}

_WIRE_TYPE_NAME = {
    0: 'varint',
    1: 'fixed64',
    2: 'length_delimited',
    3: 'start_group',
    4: 'end_group',
    5: 'fixed32',
}


def _integer_text(value):
    # str(int) 对 64 位无符号值也是完整十进制文本，不经过 float。
    return str(int(value))


def _scalar_value(field, value):
    field_type = field.type
    if field_type in _INTEGER64_TYPES:
        return _integer_text(value)
    if field_type == FieldDescriptor.TYPE_BYTES:
        return base64.b64encode(value).decode('ascii')
    if field_type == FieldDescriptor.TYPE_ENUM:
        enum_descriptor = field.enum_type
        known = enum_descriptor.values_by_number.get(value)
        return {
            'kind': 'enum',
            'number': int(value),
            'name': known.name if known is not None else None,
            'unknown_enum': known is None,
        }
    if field_type in (FieldDescriptor.TYPE_FLOAT, FieldDescriptor.TYPE_DOUBLE):
        text = repr(float(value))
        if text in ('nan', 'inf', '-inf'):
            return {'kind': _SCALAR_KIND[field_type], 'special': text}
        return float(value)
    # int32 系列、bool、string 都是 JSON 原生可无损表达的类型。
    return value


def _decode_message(message):
    descriptor = message.DESCRIPTOR
    present_numbers = {field.number for field, _ in message.ListFields()}
    real_oneofs = {oneof.name for oneof in descriptor.oneofs}
    field_views = {}
    absent = []

    for field in descriptor.fields:
        if field.number not in present_numbers:
            absent.append(field.name)
            continue
        field_views[field.name] = _decode_field(message, field, real_oneofs)

    return {
        'type': descriptor.full_name,
        'fields': field_views,
        'absent': absent,
        'unknown': _decode_unknown(message),
    }


def _decode_field(message, field, real_oneofs):
    if field.label == FieldDescriptor.LABEL_REPEATED:
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            return _decode_map(message, field)
        return {
            'kind': 'repeated',
            'field_type': _field_type_name(field),
            'values': [_decode_single(message, field, item)
                       for item in getattr(message, field.name)],
        }

    view = {
        'kind': 'scalar' if field.message_type is None else 'message',
        'field_type': _field_type_name(field),
        'value': _decode_single(message, field, getattr(message, field.name)),
        'present': True,
    }
    oneof = field.containing_oneof
    if oneof is not None and oneof.name in real_oneofs:
        view['oneof'] = oneof.name
        view['oneof_choice'] = field.name
    return view


def _decode_single(message, field, value):
    if field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
        return _decode_message(value)
    return _scalar_value(field, value)


def _decode_map(message, field):
    entry = field.message_type
    key_field = entry.fields_by_name['key']
    value_field = entry.fields_by_name['value']
    entries = []
    for key, value in getattr(message, field.name).items():
        entries.append({
            'key': _scalar_value(key_field, key),
            'value': _decode_message(value)
            if value_field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE
            else _scalar_value(value_field, value),
        })
    return {
        'kind': 'map',
        'field_type': _field_type_name(field),
        'entries': entries,
    }


def _field_type_name(field):
    if field.type == FieldDescriptor.TYPE_MESSAGE or field.type == FieldDescriptor.TYPE_GROUP:
        return field.message_type.full_name
    if field.type == FieldDescriptor.TYPE_ENUM:
        return field.enum_type.full_name
    return _SCALAR_KIND[field.type]


def _decode_unknown(message):
    try:
        unknown_set = unknown_fields_mod.UnknownFieldSet(message)
    except AttributeError:
        return []
    result = []
    for unknown in unknown_set:
        wire_type = unknown.wire_type
        entry = {
            'field_number': unknown.field_number,
            'wire_type': wire_type,
            'wire_type_name': _WIRE_TYPE_NAME.get(wire_type, str(wire_type)),
        }
        data = unknown.data
        if wire_type == 2:
            entry['data_base64'] = base64.b64encode(bytes(data)).decode('ascii')
        elif wire_type in (0, 1, 5):
            entry['value'] = str(int(data)) if wire_type in (0, 1) else int(data)
        else:
            entry['data_repr'] = repr(data)
        result.append(entry)
    return result


def decode_dynamic(message):
    """解释一个由 message_factory 动态构造的消息实例。"""
    return _decode_message(message)


def decode_any_payload(bundle, type_url, payload_bytes):
    """用指定来源的描述符集合解释 Any 原文。

    返回 (message_class, decoded_view)；bundle 中没有该类型时抛 UnknownTypeError。
    """
    full_name = type_url_full_name(type_url)
    message_class = bundle.message_class(full_name)
    message = message_class()
    message.ParseFromString(bytes(payload_bytes))
    return message_class, decode_dynamic(message)


def to_canonical_json(view):
    """把结构化视图编码为 JSON 文本，保证 64 位整数等信息无损。"""
    return json.dumps(
        {'canonical_version': _CANONICAL_VERSION, 'view': view},
        ensure_ascii=False,
        separators=(',', ':'),
        allow_nan=False,
    )


def from_canonical_json(text):
    """to_canonical_json 的逆操作，返回原始结构化视图。"""
    envelope = json.loads(text)
    if envelope.get('canonical_version') != _CANONICAL_VERSION:
        raise ValueError('规范视图版本不受支持')
    return envelope['view']
