# 来源资料约定

实验室固定传输信封由 packet.proto 描述，payload 使用 google.protobuf.Any。FileDescriptorSet 通过 Describe 读取；v1 与 v2 使用相同全名但具有不同的内容指纹。packets.json 同时记录传输信封与原始插件消息的 base64，便于联调检查。未知插件条目没有配套的类型描述符。

## 无损结构化视图（typed_rpc.decoder）

视图由 dict/list/str/int/bool/float/None 组成，但不做“普通字典”式的压平，五种语义可以区分，且 `to_canonical_json`/`from_canonical_json` 一轮 JSON 往返完全无损：

- 未出现的 optional 字段不进入 `fields`，只列入 `absent`；显式零值照常出现在 `fields` 中，`present` 为真、`value` 为 0/0.0/''/False。
- oneof 只报告命中的分支，并带 `oneof` 与 `oneof_choice`；未命中分支进入 `absent`。proto3 optional 的合成 oneof 不会被当作真实 oneof。
- 枚举值保留 `number` 与（可能为空的）`name`，描述符里没有该数字时 `unknown_enum` 为真。
- 全部 64 位整数（含 repeated、map 的 int64 键值）以十进制字符串承载，避免 JSON number 精度损失；bytes 用 base64。
- 描述符不认识的字段号进入 `unknown`，保留字段号、线类型与原始数据；换更完整的描述符重新解释后即成为正常字段。

## 接收存储布局（ReceiveStore）

- `objects/`：信封与 Any 负载的内容寻址对象，文件名即 SHA-256，重复内容只存一份。
- `records.jsonl`：来源序号、type_url、两个对象摘要、描述符 id 与指纹、结构化视图与状态；重新解释追加新行，历史可追溯。
- `acks.jsonl`：下游确认的只追加日志；“已交付”和“已确认”严格分开。
- `runs.jsonl`：每次接收的结束原因（completed / cancelled / rpc_error / error）与 gRPC 状态码。
- `descriptors/` 与 `descriptors.json`：按内容指纹归档的描述符原文及其索引，供进程重启后重新登记。
