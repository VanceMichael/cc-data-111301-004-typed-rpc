# 动态 Protobuf 服务的类型感知资料接收工具

这个 Python 项目用于业务来源资料的检查、原始字节归档与按来源描述符的动态接收。不生成、也不维护每个插件的静态客户端：插件类型由来源提供的 FileDescriptorSet 动态解释，描述符可以在运行中补交。

- `typed_rpc/core.py`：单份描述符资料预览。
- `typed_rpc/descriptors.py`：按 `descriptor_id` 隔离的动态类型注册中心，独立类型池、乱序依赖处理、内容指纹与持久化。
- `typed_rpc/decoder.py`：动态消息的无损结构化解释与规范 JSON 往返。
- `typed_rpc/receiver.py`：有界、可取消、区分结束状态的 gRPC 资料流接收器。
- `typed_rpc/store.py`：原始对象、来源序号记录、下游确认与运行历史的持久化。
- `typed_rpc/reinterpret.py`：补齐描述符后从归档原字节重新解释。
- `typed_rpc/archive.py`：单文件内容寻址归档。
- `fixtures/`：本地业务演练资料（含可补交的 `unknown.pb`）。

## 环境

Python 3.11.2。依赖及其间接依赖固定在 `requirements.txt`；空清单表示只使用标准库之外的 grpcio/protobuf。安装只在环境准备阶段执行：

```sh
python3 -m pip install -r requirements.txt
```

## 运行

单份资料的预览与归档保持不变：

```sh
python3 -m typed_rpc inspect fixtures/v1.pb
python3 -m typed_rpc capture fixtures/v1.pb --archive .work/raw
```

针对本地演练服务的完整接收流程：

```sh
# 接收资料流（自动预取 v1/v2 描述符，未知插件先保留原字节）
python3 -m typed_rpc watch 127.0.0.1:PORT --store .work/stream
# 查询某条结构化记录及其使用的描述符指纹
python3 -m typed_rpc records --store .work/stream --sequence 1
# 补交原先未知的插件类型描述符并重新解释
python3 -m typed_rpc reinterpret --store .work/stream \
    --descriptor-file fixtures/unknown.pb --descriptor-id unknown
# 一条命令跑通“接收 -> 未知保留 -> 补交 -> 重新解释 -> JSON 无损核对”
python3 -m typed_rpc demo-watch
```

`watch` 在流干净耗尽时退出码为 0；服务端非 OK 结束等异常情况退出码为 2，结束原因与状态码同时写入 `runs.jsonl`，不会被记成正常结束。

## 关键语义

- 类型缓存不按消息全名全局共用：同名消息在不同来源/版本里可以有不同定义；描述符交替更新后，旧消息仍按旧指纹的定义回读。
- 不知道的 Any 保留原字节和 type_url，补齐描述符后可离线重新解释。
- 结构化视图区分未出现的 optional、显式零值、oneof 分支、枚举未知值与 64 位整数，规范 JSON 往返不损失这些信息。
- 接收队列有界，下游过慢时背压并以异常结束；取消与非 OK 状态都不是 completed。
- 下游确认单独落盘：已交付未确认的消息重播时重新投递，已确认的消息重播时幂等跳过。

## 开发检查

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q typed_rpc tests
```

## 本地演示

```sh
python3 -m typed_rpc demo
python3 -m typed_rpc demo-watch
```

演示实际读取来源资料，打印原始归档摘要、结构预览与重新解释结果，使用临时目录并自动清理。所有示例身份与业务数值均为虚构，不需要外部账号。
