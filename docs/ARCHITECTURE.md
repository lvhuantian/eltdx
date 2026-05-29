# eltdx 1.0 架构说明

## 项目说明

本仓库是 `eltdx` 的 Python 项目根目录，用于开发、测试和发布。

协议文档是 `7709` 二进制接口的证据来源，发布版以仓库内公开文档为准。

历史实现中已验证过的协议解析、测试样本和 transport 组件，已按模块整理到当前项目。

## 分层

| 层 | 目录 | 职责 |
| --- | --- | --- |
| 产品入口 | `client.py` | 暴露 `TdxClient`，挂载各业务 API |
| 业务 API | `api/` | 面向使用者的调用方法，比如快照、K线、分时 |
| 传输层 | `transport/` | socket 连接、重连、收包、请求执行 |
| 协议层 | `protocol/` | 二进制帧、命令注册、payload 编解码 |
| 模型层 | `models/` | 对外返回的数据结构 |
| 测试 | `tests/` | 单元测试、协议样本回放测试 |

## 调用链

以批量行情快照为例：

```text
用户代码
  -> TdxClient.quotes.get_snapshots(["sz000001"])
  -> QuoteApi
  -> command registry: snapshots -> 0x054c
  -> transport.execute(0x054c, payload)
  -> protocol command builder
  -> socket frame
  -> protocol parser
  -> Quote models
```

当前默认 `TdxClient()` 使用真实 7709 transport；内存实现通过 `TdxClient.in_memory()` 显式启用，用来固定 API 形状和测试行为。真实行情连接底层使用 `SocketTransport`，外层可以用 `PooledSocketTransport` 做连接池和 round-robin。单个 socket 内部已经有 reader 线程、后台心跳、pending 响应配对和 push queue。包含服务端主动推送的接口可以通过队列读取。

真实连接调用链：

```text
TdxClient() / TdxClient.from_hosts()
  -> PooledSocketTransport
  -> SocketTransport
  -> build_command_frame(command, payload, msg_id)
  -> RequestFrame.to_bytes()
  -> socket.sendall()
  -> reader thread
  -> decode_response()
  -> pending response router
  -> parse_command_response()
```

`probe_hosts=True` 时，连接池创建前会先对候选主站做 TCP connect 测速，把可连通、延迟低的主站排在前面。测速只代表 TCP 连接成功，不等于业务命令一定成功。

不传 `host` / `hosts` 时，主站列表从包内 `tdx_server.json` 读取；如果配置文件不可用，会退回代码内置列表。真实 socket 默认每 30 秒发一次 `0x0004` 心跳，保持长时间空闲连接可用；短脚本无需额外处理。

## 目录整理思路

历史实现承担了较多职责，当前项目按下面的方式重新拆分：

| 旧问题 | 新处理 |
| --- | --- |
| `client.py` 太厚，混合连接、分页、复权、业务辅助逻辑 | `TdxClient` 只做门面，业务放 `api/` |
| `protocol/model_*.py` 同时做请求构造和响应解析 | 新项目按命令模块拆成 builder / parser |
| `models.py` 所有模型在一个文件 | 新项目按业务拆到 `models/` |
| 研究脚本、工具服务和扩展计算与核心行情接口耦合 | 放到扩展或工具目录 |

## 1.0 核心优先级

首版优先接入：

| 能力 | 命令 |
| --- | --- |
| 握手 / 心跳 | `0x000d`, `0x0004` |
| 代码数量 / 代码表 | `0x044e`, `0x044d` |
| 批量快照 | `0x054c` |
| 分类行情 | `0x054b` |
| 行情增量刷新 | `0x0547` |
| K线 | `0x052d` |
| 当日分时 | `0x0537` |
| 历史分时 | `0x0fb4` |
| 当日成交明细 | `0x0fc5` |

上述主动请求类能力已经接入真实请求和解析。`0x0547` 已完成协议层构包、响应解析、单次刷新和推送队列。

第二阶段接入：

| 能力 | 命令 |
| --- | --- |
| 集合竞价 | `0x056a` |
| 历史成交明细增强 | `0x0fc6` |
| 近期历史分时 | `0x0feb` |
| 分时副图 | `0x051b` |
| 小走势图 | `0x0fd1` |
| 股本变迁 / 财务 / 涨跌停限制 | `0x000f`, `0x0010`, `0x0452` |

第二阶段主动查询类能力也已经完成首版接入。架构重点转向推送 transport、样本回放测试和发布前 API 打磨。
