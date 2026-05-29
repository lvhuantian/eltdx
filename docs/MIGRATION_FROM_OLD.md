# 历史实现整理说明

历史实现中已经验证过的协议解析、测试样本和 transport 组件，是 `eltdx 1.0` 的重要来源。当前项目把可复用能力按模块重新整理。

## 整理原则

| 原则 | 说明 |
| --- | --- |
| 先整理核心行情能力 | 代码表、快照、K 线、分时、成交明细优先 |
| 保留已验证的协议逻辑 | 已抓包确认的请求构造和响应解析优先复用 |
| 模块职责清楚 | socket、协议、业务 API、返回模型分开 |
| 研究工具独立放置 | 抓包脚本、字段探索工具放到脚本或研究目录；MCP 作为 `eltdx-mcp` 对外入口 |
| 对外 API 使用业务名 | 命令号保留在 `protocol.commands` |

## 已整理模块

| 历史模块 | 新位置 | 处理方式 |
| --- | --- | --- |
| `protocol/frame.py` | `src/eltdx/protocol/frame.py` | 基本复用 |
| `transport/router.py` | `src/eltdx/transport/router.py` | 按当前 reader / pending 机制重整 |
| `transport/reader.py` | `src/eltdx/transport/socket.py` | 合并到 socket transport |
| `transport/heartbeat.py` | `src/eltdx/transport/socket.py` | 合并到 session / transport |
| `transport/connection.py` | `src/eltdx/transport/socket.py` | 重构后整理 |
| `protocol/model_*.py` | `src/eltdx/protocol/commands/` | 拆成请求构造和响应解析 |
| `models.py` | `src/eltdx/models/` | 按业务拆分 |

## 独立模块

| 内容 | 当前处理 |
| --- | --- |
| 7615 F10 / HTTP 接口 | 已作为 `eltdx.f10` / `client.f10` 接入 |
| 复权衍生计算 | `src/eltdx/equity.py`，并通过 `client.get_factors()` 等方法暴露 |
| 抓包和字段研究脚本 | `scripts/` 或研究目录 |
| MCP 服务 | 已作为 `src/eltdx/mcp.py` 和 `eltdx-mcp` 接入 |

## 已补回的旧版便捷入口

新项目主推 `client.quotes.get_snapshots()` 这类分组 API，同时保留一批旧版常用方法，方便历史代码接入。

| 旧版入口 | 当前处理 |
| --- | --- |
| `get_quote()` | 已补回，自动拆批后调用 `0x054c` |
| `get_count()` / `get_codes()` / `get_codes_all()` | 已补回，调用代码表 API |
| `get_stock_codes_all()` / `get_a_share_codes_all()` / `get_etf_codes_all()` / `get_index_codes_all()` | 已补回，基于 `SecurityCode.category` 本地过滤 |
| `get_kline()` / `get_kline_all()` | 已补回，调用 `0x052d`，支持两种参数顺序 |
| `get_adjusted_kline()` / `get_adjusted_kline_all()` | 已补回名称，使用 `0x052d` 服务端复权参数 |
| `get_minute()` / `get_history_minute()` | 已补回，调用当日 / 历史分时 |
| `get_trades()` / `get_trades_all()` | 已补回，支持当日 / 历史成交明细和分页聚合 |
| `get_trade()` / `get_trade_all()` / `get_history_trade()` / `get_history_trade_day()` | 已补回为成交明细别名 |
| `get_call_auction()` / `get_auction_0925()` | 已补回，后者从历史成交明细提取 09:25 |
| `get_gbbq()` | 已补回，包装新版 `capital_changes()` |
| `get_xdxr()` | 已补回，从 `get_gbbq()` 过滤除权除息记录 |
| `get_equity_changes()` / `get_equity()` | 已补回，从股本变迁记录整理股本 |
| `get_turnover()` | 已补回，用成交量和流通股本本地计算 |
| `get_factors()` | 已补回，用不复权日 K 和除权除息记录计算本地复权因子 |
| 低频数据缓存 | 已补回，缓存代码表、股本变迁和财务完整结果 |
| `to_jsonable()` / `to_json()` | 已补回，支持 dataclass、日期和 bytes |
| 默认真实连接 | 已恢复；`TdxClient.in_memory()` 保留给测试 |
| 连接池 / 主站测速 | 已补回首版，使用 `PooledSocketTransport` 和 TCP connect 测速 |

旧字段到新字段的对照见 [FIELD_MIGRATION.md](FIELD_MIGRATION.md)。新版主要提供 `raw_payload` 和单条 `record_hex`。

## 验收标准

每接入一个 `7709` 命令，至少满足：

1. 有请求构造。
2. 有响应解析。
3. 有样本回放测试。
4. 字段名和中文含义能追溯到仓库内协议文档。
5. 对外 API 返回稳定模型，底层二进制帧留在协议层处理。
