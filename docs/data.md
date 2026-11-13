# 来源资料约定

实验室固定传输信封由 packet.proto 描述，payload 使用 google.protobuf.Any。FileDescriptorSet 通过 Describe 读取；v1 与 v2 使用相同全名但具有不同的内容指纹。packets.json 同时记录传输信封与原始插件消息的 base64，便于联调检查。未知插件条目没有配套的类型描述符。
