DESCRIPTION = '动态 Protobuf 服务的类型感知资料接收'
SAMPLE = 'fixtures/v1.pb'

import argparse
import json
from pathlib import Path
import tempfile

import grpc

from .archive import archive_file
from .core import inspect_file
from .decoder import from_canonical_json, to_canonical_json
from .descriptors import DescriptorRegistry
from .receiver import Receiver
from .reinterpret import reinterpret_pending
from .rpc_source import rpc_source
from .store import ReceiveStore


def _print(payload):
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _build_registry(store_dir, descriptor_dir):
    registry = DescriptorRegistry()
    index_path = Path(store_dir) / 'descriptors.json'
    if index_path.exists():
        registry.load(descriptor_dir, json.loads(index_path.read_text('utf-8')))
    return registry


def _save_registry(registry, store_dir, descriptor_dir):
    index = registry.save(descriptor_dir)
    (Path(store_dir) / 'descriptors.json').write_text(
        json.dumps(index, ensure_ascii=False, indent=2), encoding='utf-8')
    return index


def cmd_watch(args):
    registry = _build_registry(args.store, Path(args.store) / 'descriptors')
    store = ReceiveStore(args.store)
    channel = grpc.insecure_channel(args.address)
    try:
        receiver = Receiver(channel, registry, store,
                            queue_capacity=args.queue_capacity)
        receiver.preload_descriptors(args.descriptor_id or ['v1', 'v2'])

        def handler(delivery):
            # 本地演练下游：处理后立即确认；未补齐描述符的消息不确认，等待补交。
            if delivery.status == 'decoded':
                delivery.ack(store, downstream=args.downstream)

        request = json.dumps({'fail_end': args.fail_end}).encode('utf-8')
        result = receiver.run(request, handler=handler)
        descriptor_index = _save_registry(
            registry, args.store, Path(args.store) / 'descriptors')
        payload = {
            'run': {
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
            },
            'descriptors': descriptor_index,
            'records': store.list_records(),
        }
        _print(payload)
        return 0 if result.ended == 'completed' else 2
    finally:
        channel.close()


def cmd_records(args):
    store = ReceiveStore(args.store)
    records = store.list_records()
    if args.sequence is not None:
        records = [record for record in records
                   if record['sequence'] == str(args.sequence)]
    if args.canonical_json:
        canonical = []
        for record in records:
            record = dict(record)
            if record['decoded'] is not None:
                # 输出完整规范信封：从这段文本再读回来与内存视图完全一致。
                record['decoded'] = json.loads(
                    to_canonical_json(record['decoded']))
            canonical.append(record)
        records = canonical
    _print({'records': records, 'acks': store.list_acks(),
            'runs': store.list_runs()})
    return 0


def cmd_reinterpret(args):
    store = ReceiveStore(args.store)
    registry = _build_registry(args.store, Path(args.store) / 'descriptors')
    if args.descriptor_file:
        for descriptor_file in args.descriptor_file:
            raw = Path(descriptor_file).read_bytes()
            descriptor_id = args.descriptor_id or Path(descriptor_file).stem
            registry.register(descriptor_id, raw)
    updated, pending = reinterpret_pending(store, registry)
    _save_registry(registry, args.store, Path(args.store) / 'descriptors')
    _print({
        'reinterpreted': [
            {'source': record['source'], 'sequence': record['sequence'],
             'descriptor_id': record['descriptor_id'],
             'descriptor_fingerprint': record['descriptor_fingerprint'],
             'decoded': record['decoded']}
            for record in updated
        ],
        'still_pending': [
            {'source': record['source'], 'sequence': record['sequence'],
             'type_url': record['type_url'], 'decode_error': record['decode_error']}
            for record in pending
        ],
    })
    return 0 if not pending else 1


def cmd_demo_watch(args):
    with tempfile.TemporaryDirectory() as work, \
            rpc_source('fixtures') as address:
        args.address = address
        args.store = work
        args.fail_end = False
        args.queue_capacity = 4
        args.descriptor_id = ['v1', 'v2']
        args.downstream = 'local-drill'
        watch_code = cmd_watch(args)
        store = ReceiveStore(work)
        # 补交原先未知的插件类型描述符，重新解释第 3 条消息。
        registry = _build_registry(work, Path(work) / 'descriptors')
        registry.register('unknown', Path('fixtures/unknown.pb').read_bytes())
        updated, pending = reinterpret_pending(store, registry)
        _save_registry(registry, work, Path(work) / 'descriptors')
        roundtrip_ok = all(
            from_canonical_json(to_canonical_json(record['decoded']))
            == record['decoded']
            for record in store.list_records() if record['decoded'] is not None)
        _print({
            'watch_exit_code': watch_code,
            'reinterpreted': updated,
            'still_pending': pending,
            'canonical_json_roundtrip_lossless': roundtrip_ok,
        })
        return 0 if watch_code == 0 and len(updated) == 1 and not pending else 1


def main():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    sub = parser.add_subparsers(dest='command')

    inspect_parser = sub.add_parser('inspect')
    inspect_parser.add_argument('path', nargs='?', default=SAMPLE)

    capture_parser = sub.add_parser('capture')
    capture_parser.add_argument('path', nargs='?', default=SAMPLE)
    capture_parser.add_argument('--archive', default='.work/raw')

    sub.add_parser('demo')

    watch_parser = sub.add_parser('watch')
    watch_parser.add_argument('address')
    watch_parser.add_argument('--store', required=True)
    watch_parser.add_argument('--queue-capacity', type=int, default=8)
    watch_parser.add_argument('--descriptor-id', action='append')
    watch_parser.add_argument('--downstream', default='local-drill')
    watch_parser.add_argument('--fail-end', action='store_true')

    records_parser = sub.add_parser('records')
    records_parser.add_argument('--store', required=True)
    records_parser.add_argument('--sequence')
    records_parser.add_argument('--canonical-json', action='store_true')

    reinterpret_parser = sub.add_parser('reinterpret')
    reinterpret_parser.add_argument('--store', required=True)
    reinterpret_parser.add_argument('--descriptor-file', action='append')
    reinterpret_parser.add_argument('--descriptor-id')

    demo_watch_parser = sub.add_parser('demo-watch')
    demo_watch_parser.set_defaults(handler=cmd_demo_watch)

    args = parser.parse_args()

    if args.command == 'inspect':
        _print(inspect_file(Path(args.path)))
        return
    if args.command == 'capture':
        _print(archive_file(Path(args.path), Path(args.archive)))
        return
    if args.command == 'demo':
        with tempfile.TemporaryDirectory() as directory:
            result = {'原始资料': archive_file(Path(SAMPLE), Path(directory)),
                      '资料预览': inspect_file(Path(SAMPLE))}
        _print(result)
        return
    if args.command == 'watch':
        raise SystemExit(cmd_watch(args))
    if args.command == 'records':
        raise SystemExit(cmd_records(args))
    if args.command == 'reinterpret':
        raise SystemExit(cmd_reinterpret(args))
    if args.command == 'demo-watch':
        raise SystemExit(cmd_demo_watch(args))
    parser.print_help()


if __name__ == '__main__':
    main()
