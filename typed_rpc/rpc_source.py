from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import base64
import json
import time

import grpc


@contextmanager
def rpc_source(directory):
    root = Path(directory)
    packets = json.loads((root / 'packets.json').read_text(encoding='utf-8'))

    def describe(request, context):
        name = request.decode() or 'v1'
        if name not in {'v1', 'v2'}:
            context.abort(grpc.StatusCode.NOT_FOUND, '描述符不存在')
        return (root / (name + '.pb')).read_bytes()

    def watch(request, context):
        options = json.loads(request or b'{}')
        for index, packet in enumerate(packets):
            if not context.is_active():
                return
            yield base64.b64decode(packet['wire'])
            if options.get('hang_after') and index + 1 >= options['hang_after']:
                # 演练用：发出指定条数后挂起，便于确定性地测试取消与背压。
                while context.is_active():
                    time.sleep(0.02)
                return
        if options.get('fail_end'):
            context.abort(grpc.StatusCode.UNAVAILABLE, '资料源暂时中断')

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('source.Catalog', {
        'Describe':grpc.unary_unary_rpc_method_handler(describe),
        'Watch':grpc.unary_stream_rpc_method_handler(watch),
    }),))
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    try:
        yield '127.0.0.1:' + str(port)
    finally:
        server.stop(0).wait()
