# 更新日志

这里记录 `eltdx` 的公开版本变化。

## 未发布

### 新增

- 补全 `eltdx-mcp` 工具集，新增分时、逐笔、竞价、代码表、股本、复权因子和换手率等 MCP 工具

## 0.5.0 - 2026-05-12

### 新增

- 内置 `tdx_server.json` 默认服务器列表，列表已按本地 TCP 连接测速结果由快到慢排序
- 新增 `probe_hosts`、`probe_timeout`、`probe_workers` 参数，可在客户端初始化时对候选服务器重新测速并按延迟重排
- 新增 `to_jsonable()` / `to_json()`，方便把 dataclass、日期时间、枚举等结果转换成 JSON-friendly 结构
- 新增可选 `eltdx[mcp]` 依赖和 `eltdx-mcp` 命令，当前暴露 `tdx_get_kline` 与 `tdx_get_quote` 两个 MCP 工具

### 修改

- `TdxClient` 默认改用库内服务器列表；连接池会按最快服务器顺序错位分配，减少批量请求集中到单一地址
- 加固连接生命周期和请求串行化，避免重连、心跳、读线程与并发请求互相影响
- MCP K 线工具的 `adjust` 参数支持大小写和空白容错，例如 `" QFQ "` 会规范为 `"qfq"`
- 重写 README 首页，增加能力概览、适用场景、服务器选择、MCP 工具、注意事项和文档地图，方便新用户快速理解项目
- 补充 MCP、服务器测速、连接回滚、JSON 序列化相关文档与单元测试

### 修复

- 修复连接池启动时某条连接失败后，已建立连接没有回滚关闭的问题，避免留下半连接状态

## 0.4.1 - 2026-04-13

### 修复

- 修复 `get_auction_0925()` 对历史逐笔 probe 空包 `0000` 的误判；这类响应现在按“0 笔数据”处理，不再抛出 `ProtocolError`
- 补充两字节空 payload 的单元测试，锁定老票老日期场景下的 `09:25` 空结果行为

## 0.4.0 - 2026-04-13

### 新增

- 新增 `get_auction_0925(code, date)`，用于快速定位历史逐笔里的 `09:25` 那一笔，适合批量导出竞价价、竞价量和竞价额
- 新增 `Auction0925Result` 返回模型，统一返回 `has_auction_0925`、成交价、成交量、成交额以及探测页数等字段
- 新增 `scripts/smoke/export_auction_925_daily.py`，可按交易日导出 `09:25` CSV

### 修改

- 历史 `09:25` 探测链路增加规范化代码返回，统一输出 `sh` / `sz` 前缀完整代码
- 历史逐笔 probe 增加坏包最小头校验，避免把截断响应误判成“无数据”
- `get_auction_0925()` 的回退扫描增加协议页上限保护，避免异常服务端响应导致无限探测

## 0.3.1 - 2026-03-12

### 修改

- 修正 `get_call_auction()` 对集合竞价记录中撮合量与未撮合量的解析，改为按 4 字节字段读取，避免大单量场景被截断
- 补充覆盖大于 16 位范围的集合竞价单元测试，防止 `unmatched` 再次被误解析

## 0.3.0 - 2026-03-08

### 修改

- 为 `get_quote()` 的 `Quote` 增加 `call_auction_amount` 和 `call_auction_rate` 两个集合竞价相关字段
- 将 `Quote.intuition` 重命名为 `Quote.current_hand`，并将字段语义明确为“现手数（现量）”

## 0.2.0 - 2026-03-08

### 修改

- 修正 `get_quote()` 中快照价格字段的语义映射：`last_price` 对应最新价，`last_close_price` 对应昨收 / 前收价
- 将 `Quote.close_price` / `Quote.close_price_milli` 重命名为 `Quote.last_close_price` / `Quote.last_close_price_milli`
- 同步更新快照相关代码、测试、示例和字段说明，统一快照命名口径

## 0.1.4 - 2026-03-07

主要修正 PyPI 展示和发布元数据。

### 修改

- 修复 `pyproject.toml` 中的包摘要文字，避免 PyPI 顶部简介显示异常
- 补充 `docs/README.md` 与 `scripts/README.md` 的双向导航，方便用户在文档与脚本之间切换
- 更新 README 中的 PyPI 版本徽章缓存参数，方便新版发布后更快刷新

## 0.1.3 - 2026-03-07

修正文档导航与 PyPI 展示细节。

### 修改

- 调整 README 首页文案，优化 PyPI 页面阅读体验
- 统一仓库首页、GitHub About 和 PyPI 展示用链接
- 整理 `scripts/` 目录与说明文档，便于查找 smoke 和 validation 脚本

## 0.1.2 - 2026-03-07

继续完善发布资料与文档说明。

### 修改

- 修正 PyPI 项目页显示用 README 内容
- README 中明确 Python 版本要求为 `Python 3.10+`
- 补充 `docs/API_REFERENCE.md` 的字段说明和使用示例

## 0.1.1 - 2026-03-07

修复 Python 3.10 环境兼容问题。

### 修改

- 处理 Python 3.10 下缺少 `math.exp2()` 的兼容逻辑
- 调整 CI 与构建验证，覆盖 Python 3.10
- 确保 wheel 可在 Python 3.10 环境正常安装导入

## 0.1.0 - 2026-03-07

首个公开版本，提供统一的行情客户端接口。

### 主要能力

- 提供 `TdxClient` 统一入口
- 默认使用 `pool_size=2` 的连接池
- `get_quote()` 内置自动分批，默认 `batch_size=80`
- 支持 `with TdxClient() as client:` 上下文管理
- 返回模型统一为 dataclass，时间字段转换为 Python 原生 `datetime` / `date`
- 支持 `include_raw=True` 查看原始十六进制数据

### 首批接口

- `get_call_auction()`
- `get_quote()`
- `get_count()`
- `get_codes()`
- `get_codes_all()`
- `get_stock_codes_all()`
- `get_etf_codes_all()`
- `get_index_codes_all()`
- `get_minute()`
- `get_trades()`
- `get_trades_all()`
- `get_kline()`
- `get_kline_all()`

### 兼容说明

- 提供 `get_history_minute()`、`get_trade()`、`get_trade_all()`、`get_history_trade()`、`get_history_trade_day()` 等兼容别名
- 兼容旧的 API 调用顺序，例如 `get_kline(freq, code)` 与 `get_kline(code, freq)`

### 测试

- 提供单元测试与可选联网集成测试
- 联网测试可通过 `ELTDX_RUN_LIVE=1` 开启
