<div align="center">
  <h1>eltdx</h1>
  <p><strong>通达信在线行情协议的 Python SDK</strong></p>
  <p>读取 A 股快照、分时、逐笔、K 线、集合竞价、历史 09:25 竞价和复权相关数据。</p>
  <p>
    <a href="https://pypi.org/project/eltdx/"><img alt="PyPI" src="https://img.shields.io/pypi/v/eltdx?label=pypi&logo=pypi"></a>
    <a href="https://pypi.org/project/eltdx/"><img alt="Python 3.10+" src="https://img.shields.io/badge/Python-3.10%2B-blue"></a>
    <a href="https://github.com/electkismet/eltdx/actions/workflows/ci.yml"><img alt="Build" src="https://img.shields.io/github/actions/workflow/status/electkismet/eltdx/ci.yml?branch=main&label=build"></a>
    <a href="https://github.com/electkismet/eltdx/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/pypi/l/eltdx?label=license"></a>
  </p>
</div>

## 简介

`eltdx` 把常用的通达信在线行情协议封装成一个 `TdxClient`。你不需要安装通达信客户端，也不需要自己处理 socket、协议包和字段换算。

它主要解决三件事：

- 常用行情接口直接调用：快照、分时、逐笔、K 线、集合竞价、历史 `09:25`。
- 返回结构尽量稳定：结果是 dataclass，时间是 `date` / `datetime`，价格同时保留浮点值和 `*_milli` 整数值。
- 连接更省心：内置默认服务器列表，支持连接池、批量快照分发和可选测速选优。

它不是财报 / F10 / 公告下载器，也不读取本地 `vipdoc`、`.day`、`.lc1` 文件。当前重点是沪深北市场的在线行情数据。

## 安装

```bash
python -m pip install eltdx
```

源码开发：

```bash
git clone https://github.com/electkismet/eltdx.git
cd eltdx
python -m pip install -e ".[dev]"
```

要求 Python `3.10+`。

## 快速开始

### 快照行情

```python
from eltdx import TdxClient

with TdxClient() as client:
    quotes = client.get_quote(["sz000001", "sh600000"])

for quote in quotes:
    print(quote.code, quote.last_price, quote.last_close_price, quote.server_time)
```

### K 线

```python
from eltdx import TdxClient, to_jsonable

with TdxClient() as client:
    kline = client.get_kline("sz000001", "day", count=5)

print(kline.count)
print(kline.items[-1].close_price)

# 需要给 Web API、CLI 或 MCP 工具返回时，可以转成可 JSON 序列化结构。
payload = to_jsonable(kline)
print(payload["items"][-1])
```

### 历史 09:25 竞价

```python
from eltdx import TdxClient

with TdxClient() as client:
    row = client.get_auction_0925("000001", "2026-04-09")

print(row.code, row.trading_date, row.has_auction_0925)
print(row.price, row.volume, row.amount)
```

## 主要接口

| 数据 | 方法 | 说明 |
| --- | --- | --- |
| 行情快照 | `get_quote()` | 最新价、昨收、今开、最高、最低、五档盘口、成交量额等 |
| 代码表 | `get_codes()` / `get_codes_all()` | 底层代码表，里面不只股票 |
| 常用代码清单 | `get_a_share_codes_all()` / `get_etf_codes_all()` / `get_index_codes_all()` | 对代码表做常用过滤 |
| 分时 | `get_minute()` / `get_history_minute()` | 实时分时和历史分时 |
| 逐笔 | `get_trades()` / `get_trades_all()` | 实时逐笔、历史逐笔、自动翻页 |
| K 线 | `get_kline()` / `get_kline_all()` | 支持分钟、日、周、月等周期 |
| 复权 K 线 | `get_adjusted_kline()` / `get_adjusted_kline_all()` | 支持前复权 `qfq` 和后复权 `hfq` |
| 集合竞价 | `get_call_auction()` | 集合竞价序列 |
| 历史 09:25 | `get_auction_0925()` | 快速定位指定交易日 09:25 那一笔 |
| 公司行为 / 股本 | `get_gbbq()` / `get_xdxr()` / `get_equity()` / `get_factors()` | 除权除息、股本变化、复权因子 |

更完整的参数和字段说明见 [API_REFERENCE.md](docs/API_REFERENCE.md) 和 [FIELD_REFERENCE.md](docs/FIELD_REFERENCE.md)。

## 服务器

不传服务器时，`eltdx` 会使用包内的 `tdx_server.json`。这个文件只保留必要字段，列表按最近一次本地测速结果从快到慢排列。

```python
from eltdx import TdxClient

with TdxClient() as client:
    quote = client.get_quote("sz000001")[0]
    print(quote.last_price)
```

如果你希望按当前网络重新测速，可以打开 `probe_hosts=True`。测速只在创建 client 时发生，不会每次请求都测。

```python
with TdxClient(probe_hosts=True) as client:
    print(client.get_quote("sz000001")[0].last_price)
```

也可以手动传服务器：

```python
hosts = ["116.205.183.150:7709", "116.205.171.132:7709"]

with TdxClient(hosts=hosts, pool_size=2, timeout=8.0) as client:
    print(client.get_quote(["sz000001", "sh600000"]))
```

常用初始化参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `host` | `None` | 指定单个服务器 |
| `hosts` | `None` | 指定多个服务器，优先级高于 `host` |
| `pool_size` | `2` | 连接池大小 |
| `batch_size` | `80` | `get_quote()` 自动分批大小，上限为 `80` |
| `probe_hosts` | `False` | 初始化时对候选服务器测速并重排 |
| `timeout` | `8.0` | socket 请求超时秒数 |

## MCP

`eltdx` 带了一个可选的 MCP server。普通 SDK 使用不需要安装 MCP 依赖；只有需要给 MCP 客户端或 Agent 调用时再安装 extra。

```bash
python -m pip install "eltdx[mcp]"
eltdx-mcp
```

当前暴露两个工具：

| 工具 | 作用 |
| --- | --- |
| `tdx_get_kline` | 读取一页 K 线，返回可 JSON 序列化结构 |
| `tdx_get_quote` | 读取一只或多只证券的实时行情快照 |

后续如果要加分时、逐笔或竞价工具，也会继续放在同一个 `eltdx-mcp` 服务里。

## 容易误解的地方

### `get_count()` 不是股票总数

`get_count("sh")` 读的是通达信代码表条目数，不是股票数量。只关心 A 股时，用：

```python
client.get_a_share_count("sh")
client.get_a_share_codes_all()
```

### `get_codes()` 不只返回股票

底层代码表会混有股票、指数、ETF、基金、债券回购、板块分类项等。常用过滤方法有：

```python
client.get_a_share_codes_all()
client.get_etf_codes_all()
client.get_index_codes_all()
```

### 代码建议带市场前缀

推荐写完整代码，例如 `sz000001`、`sh600000`、`bj920001`。部分接口可以自动补前缀，但完整代码更少歧义。

### 原始协议数据可以保留

排查协议解析问题时，可以打开 `include_raw=True`：

```python
with TdxClient() as client:
    minute = client.get_minute("sz000001", include_raw=True)
    print(minute.raw_frame_hex)
    print(minute.raw_payload_hex)
```

## 文档

| 文档 | 内容 |
| --- | --- |
| [docs/API_REFERENCE.md](docs/API_REFERENCE.md) | API 参数和返回值 |
| [docs/EXAMPLES.md](docs/EXAMPLES.md) | 可直接复制的示例 |
| [docs/FIELD_REFERENCE.md](docs/FIELD_REFERENCE.md) | 字段含义和口径 |
| [docs/DEBUG_GUIDE.md](docs/DEBUG_GUIDE.md) | 连接、服务器和 raw 数据排查 |
| [scripts/README.md](scripts/README.md) | smoke / validation 脚本说明 |

## 开发验证

```bash
python -m pytest tests/unit
python -m build
```

联网测试需要真实服务器：

```bash
set ELTDX_RUN_LIVE=1
python -m pytest tests/integration
```

## 参考

- 感谢 [injoyai/tdx](https://github.com/injoyai/tdx) 的启发。

## 联系方式

- QQ 群：[点击链接加入群聊](https://qm.qq.com/q/zAjpZsvfzy)
- 邮箱：`dapaoxixixi@163.com`

## 许可证

MIT
