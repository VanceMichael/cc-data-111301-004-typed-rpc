"""补齐描述符后对归档消息的重新解释。

原始信封与 Any 负载字节在首次接收时就已经内容寻址归档，因此这里完全不依赖
来源是否还在线：从存储里取回原字节，用当前注册的描述符重新走一遍解码，并把
新的结构化视图连同描述符指纹写回记录。每条记录都能回答“我是用哪组描述符解
出来的”。
"""

from .decoder import decode_any_payload
from .descriptors import UnknownTypeError


def reinterpret_record(store, registry, record):
    """重新解释单条记录，返回更新后的记录。"""
    bundle = registry.get(record['descriptor_id'])
    if bundle is None:
        return store.update_view(
            record['source'], record['sequence'],
            status='pending_descriptor',
            decoded=None,
            descriptor_id=record['descriptor_id'],
            fingerprint=None,
            error='来源描述符尚未登记：' + record['descriptor_id'],
        )
    payload_bytes = store.get_object(record['payload_sha256'])
    try:
        _, decoded = decode_any_payload(
            bundle, record['type_url'], payload_bytes)
    except UnknownTypeError as error:
        return store.update_view(
            record['source'], record['sequence'],
            status='pending_descriptor',
            decoded=None,
            descriptor_id=record['descriptor_id'],
            fingerprint=bundle.fingerprint,
            error=str(error),
        )
    return store.update_view(
        record['source'], record['sequence'],
        status='decoded',
        decoded=decoded,
        descriptor_id=record['descriptor_id'],
        fingerprint=bundle.fingerprint,
    )


def reinterpret_pending(store, registry):
    """重新解释所有尚未解码的记录，返回 (updated, still_pending) 两个列表。"""
    updated = []
    pending = []
    for record in store.list_records():
        if record['status'] == 'decoded':
            continue
        refreshed = reinterpret_record(store, registry, record)
        if refreshed['status'] == 'decoded':
            updated.append(refreshed)
        else:
            pending.append(refreshed)
    return updated, pending
