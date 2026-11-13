# 实验室资料接口

`typed_rpc.rpc_source.rpc_source` 启动回环地址的真实 gRPC 服务。`/source.Catalog/Describe` 接收 UTF-8 的 v1 或 v2，返回 FileDescriptorSet 原始字节。`/source.Catalog/Watch` 接收 JSON 选项，逐条返回 packet.proto 中 Packet 的原始字节。信封中 source、sequence、descriptor_id 是来源属性，payload 为 Any。消息同时登记在 fixtures/packets.json 中用于联调。

`fail_end` 为真时，发完已有资料后以 UNAVAILABLE 结束。固定信封的 FileDescriptorProto 另存于 packet.pb；插件类型描述符是 v1.pb 和 v2.pb。接口不保存消费者确认，也不把序号当作下游已经接收。
