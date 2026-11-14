# 实验室资料接口

`typed_rpc.rpc_source.rpc_source` 启动回环地址的真实 gRPC 服务。`/source.Catalog/Describe` 接收 UTF-8 的 v1 或 v2，返回 FileDescriptorSet 原始字节。`/source.Catalog/Watch` 接收 JSON 选项，逐条返回 packet.proto 中 Packet 的原始字节。信封中 source、sequence、descriptor_id 是来源属性，payload 为 Any。消息同时登记在 fixtures/packets.json 中用于联调。

`fail_end` 为真时，发完已有资料后以 UNAVAILABLE 结束；`hang_after` 为正整数时，发出该条数的消息后挂起，供取消与背压演练做确定性控制。固定信封的 FileDescriptorProto 另存于 packet.pb；插件类型描述符是 v1.pb 和 v2.pb，`fixtures/unknown.pb` 是 lab-b 那条未知插件消息的补交描述符（含未知枚举缺口、proto3 optional 与真实 oneof）。接口不保存消费者确认，也不把序号当作下游已经接收。

## 接收侧语义（typed_rpc.receiver / store / reinterpret）

- 描述符按 `descriptor_id` 各自建立独立 DescriptorPool，同名消息在不同来源或不同版本里可以有不同定义；同一 id 交替更新时旧 bundle 仍保留，旧消息按原指纹回读。
- FileDescriptorSet 的依赖文件允许乱序到达，缺失的 google/protobuf/* 由运行时已知类型补齐，其余缺失依赖显式报错而不是猜测定义。
- 原始信封与 Any 负载先按 SHA-256 内容寻址落盘，再做动态解释。解释不了的 Any 保留原字节与 type_url，状态记为 `pending_descriptor`；`reinterpret` 从归档原字节重建视图，不要求来源在线。
- 每条记录都留有 `descriptor_id` 与 `descriptor_fingerprint`，可以反查结构化结果使用的是哪一组描述符；重新解释会向 records.jsonl 追加历史行而不是覆盖。
- 下游确认单独写入 acks.jsonl。已经确认的消息在重播时只做幂等归档、不再投递；已交付但确认未持久化的消息重播时重新投递。序号相同但字节冲突会直接报错。
- 接收队列有界，下游过慢时读取线程背压，超时后以 RESOURCE_EXHAUSTED 异常结束；客户端取消记为 CANCELLED，服务端非 OK 状态记为 rpc_error，只有流干净耗尽才是 completed，三者都不会被记成正常结束。
