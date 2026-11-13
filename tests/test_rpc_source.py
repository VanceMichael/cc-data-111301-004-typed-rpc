import grpc
import unittest
from typed_rpc.rpc_source import rpc_source


class RpcSourceTests(unittest.TestCase):
    def test_describe_and_watch(self):
        with rpc_source('fixtures') as address, grpc.insecure_channel(address) as channel:
            descriptor = channel.unary_unary('/source.Catalog/Describe')(b'v1', timeout=3)
            self.assertGreater(len(descriptor), 10)
            packets = list(channel.unary_stream('/source.Catalog/Watch')(b'{}', timeout=3))
            self.assertEqual(len(packets), 3)

    def test_stream_non_ok_status(self):
        with rpc_source('fixtures') as address, grpc.insecure_channel(address) as channel:
            with self.assertRaises(grpc.RpcError) as failure:
                list(channel.unary_stream('/source.Catalog/Watch')(b'{"fail_end":true}', timeout=3))
            self.assertEqual(failure.exception.code(), grpc.StatusCode.UNAVAILABLE)
