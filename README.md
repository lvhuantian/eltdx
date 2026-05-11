<div align="center">
  <h1>eltdx</h1>
  <p><strong>通达信在线行情协议 Python SDK</strong></p>
  <p>快照、分时、逐笔、K 线、集合竞价、历史 09:25、股本和复权。</p>
  <p>
    <a href="https://pypi.org/project/eltdx/"><img alt="PyPI" src="https://img.shields.io/pypi/v/eltdx?label=pypi&logo=pypi"></a>
    <a href="https://pypi.org/project/eltdx/"><img alt="Python 3.10+" src="https://img.shields.io/badge/Python-3.10%2B-blue"></a>
    <a href="https://github.com/electkismet/eltdx/actions/workflows/ci.yml"><img alt="Build" src="https://img.shields.io/github/actions/workflow/status/electkismet/eltdx/ci.yml?branch=main&label=build"></a>
    <a href="https://github.com/electkismet/eltdx/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/pypi/l/eltdx?label=license"></a>
  </p>
</div>

## 安装

```bash
python -m pip install eltdx
```

源码安装：

```bash
git clone https://github.com/electkismet/eltdx.git
cd eltdx
python -m pip install -e ".[dev]"
```

Python `3.10+`。

## 示例

### 快照

```python
from eltdx import TdxClient

with TdxClient() as client:
    quotes = client.get_quote(["sz000001", "sh600000"])

for quote in quotes:
    print(quote.code, quote.last_price, quote.last_close_price, quote.server_time)
```

### K 线转 JSON

```python
from eltdx import TdxClient, to_jsonable

with TdxClient() as client:
    payload = to_jsonable(client.get_kline("sz000001", "day", count=5))

print(payload["count"])
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

## 边界

| 覆盖 | 不覆盖 |
| --- | --- |
| 沪深北在线行情 | 财报 / F10 / 公告下载解析 |
| 快照、分时、逐笔、K 线 | 本地 `vipdoc`、`.day`、`.lc1` 文件解析 |
| 集合竞价、历史 09:25 | 港美股行情 |
| 股本变化、复权因子 | 交易下单 |

## 服务器

不传服务器时，默认使用包内 `tdx_server.json`。列表已按最近一次本地测速结果从快到慢排列。

```python
from eltdx import TdxClient

with TdxClient() as client:
    quote = client.get_quote("sz000001")[0]
    print(quote.last_price)
```

需要按当前网络重新测速时，打开 `probe_hosts=True`。测速只在创建 client 时发生。

```python
with TdxClient(probe_hosts=True) as client:
    print(client.get_quote("sz000001")[0].last_price)
```

手动指定服务器：

```python
hosts = ["116.205.183.150:7709", "116.205.171.132:7709"]

with TdxClient(hosts=hosts, pool_size=2, timeout=8.0) as client:
    print(client.get_quote(["sz000001", "sh600000"]))
```

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `host` | `None` | 指定单个服务器 |
| `hosts` | `None` | 指定多个服务器，优先级高于 `host` |
| `pool_size` | `2` | 连接池大小 |
| `batch_size` | `80` | `get_quote()` 自动分批大小，上限为 `80` |
| `probe_hosts` | `False` | 初始化时对候选服务器测速并重排 |
| `timeout` | `8.0` | socket 请求超时秒数 |

## MCP

普通 SDK 不依赖 MCP。需要给 MCP 客户端或 Agent 调用时再安装 extra：

```bash
python -m pip install "eltdx[mcp]"
eltdx-mcp
```

| 工具 | 作用 |
| --- | --- |
| `tdx_get_kline` | 读取一页 K 线，返回可 JSON 序列化结构 |
| `tdx_get_quote` | 读取一只或多只证券的实时行情快照 |

## 注意

### `get_count()` 不是股票总数

`get_count("sh")` 读的是通达信代码表条目数，不是股票数量。A 股数量和列表用：

```python
client.get_a_share_count("sh")
client.get_a_share_codes_all()
```

### `get_codes()` 不只返回股票

底层代码表会混有股票、指数、ETF、基金、债券回购、板块分类项等。常用过滤：

```python
client.get_a_share_codes_all()
client.get_etf_codes_all()
client.get_index_codes_all()
```

### 代码建议带市场前缀

推荐写完整代码，例如 `sz000001`、`sh600000`、`bj920001`。

### 原始协议数据可以保留

排查协议解析问题时用 `include_raw=True`：

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
