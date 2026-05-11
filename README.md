# eltdx

通达信在线行情协议 Python 库。可以拿沪深北 A 股的实时快照、分时、逐笔、K 线、集合竞价、历史 09:25 竞价、股本变化和复权因子。

[![PyPI](https://img.shields.io/pypi/v/eltdx?label=pypi&logo=pypi)](https://pypi.org/project/eltdx/)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue)](https://pypi.org/project/eltdx/)
[![Build](https://img.shields.io/github/actions/workflow/status/electkismet/eltdx/ci.yml?branch=main&label=build)](https://github.com/electkismet/eltdx/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/eltdx?label=license)](https://github.com/electkismet/eltdx/blob/main/LICENSE)

## 安装

```bash
pip install eltdx
```

需要 Python 3.10 或更高版本。

## 快速示例

```python
from eltdx import TdxClient

with TdxClient() as client:
    quotes = client.get_quote(["sz000001", "sh600000"])

for quote in quotes:
    print(quote.code, quote.last_price, quote.last_close_price, quote.server_time)
```

K 线：

```python
from eltdx import TdxClient

with TdxClient() as client:
    kline = client.get_kline("sz000001", "day", count=5)

for item in kline.items:
    print(item.time, item.close_price, item.volume)
```

历史 09:25 竞价：

```python
from eltdx import TdxClient

with TdxClient() as client:
    row = client.get_auction_0925("000001", "2026-04-09")

print(row.code, row.trading_date, row.has_auction_0925)
print(row.price, row.volume, row.amount)
```

## 接口

| 数据 | 方法 | 说明 |
| --- | --- | --- |
| 行情快照 | `get_quote()` | 最新价、昨收、今开、最高、最低、五档盘口、成交量额等 |
| 代码表 | `get_codes()` / `get_codes_all()` | 底层代码表，包含股票、指数、ETF、基金等 |
| 常用代码清单 | `get_a_share_codes_all()` / `get_etf_codes_all()` / `get_index_codes_all()` | 对代码表做常用过滤 |
| 分时 | `get_minute()` / `get_history_minute()` | 实时分时和历史分时 |
| 逐笔 | `get_trades()` / `get_trades_all()` | 实时逐笔、历史逐笔、自动翻页 |
| K 线 | `get_kline()` / `get_kline_all()` | 支持分钟、日、周、月等周期 |
| 复权 K 线 | `get_adjusted_kline()` / `get_adjusted_kline_all()` | 支持前复权和后复权 |
| 集合竞价 | `get_call_auction()` | 集合竞价序列 |
| 历史 09:25 | `get_auction_0925()` | 快速定位指定交易日 09:25 那一笔 |
| 公司行为 / 股本 | `get_gbbq()` / `get_xdxr()` / `get_equity()` / `get_factors()` | 除权除息、股本变化、复权因子 |

完整参数和字段说明见 [API_REFERENCE.md](docs/API_REFERENCE.md) 和 [FIELD_REFERENCE.md](docs/FIELD_REFERENCE.md)。

## 服务器选择

不传服务器地址时，会使用包内 `tdx_server.json` 的默认列表，已按测速结果排序。

```python
from eltdx import TdxClient

with TdxClient() as client:
    quote = client.get_quote("sz000001")[0]
    print(quote.last_price)
```

需要重新测速时打开 `probe_hosts=True`：

```python
from eltdx import TdxClient

with TdxClient(probe_hosts=True) as client:
    print(client.get_quote("sz000001")[0].last_price)
```

手动指定服务器：

```python
from eltdx import TdxClient

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

普通 SDK 不依赖 MCP。需要给 MCP 客户端或 Agent 用时：

```bash
pip install "eltdx[mcp]"
eltdx-mcp
```

| 工具 | 作用 |
| --- | --- |
| `tdx_get_kline` | 读 K 线，返回可 JSON 序列化结构 |
| `tdx_get_quote` | 读实时行情快照 |

## 注意

**`get_count()` 不是股票总数**

`get_count("sh")` 读的是通达信代码表条目数，不是股票数量。A 股数量和列表：

```python
client.get_a_share_count("sh")
client.get_a_share_codes_all()
```

**`get_codes()` 不只返回股票**

底层代码表会混有股票、指数、ETF、基金、债券回购、板块分类项等。常用过滤：

```python
client.get_a_share_codes_all()
client.get_etf_codes_all()
client.get_index_codes_all()
```

**代码建议带市场前缀**

推荐写完整代码，例如 `sz000001`、`sh600000`、`bj920001`。

**原始协议数据可以保留**

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

## 开发

```bash
pytest tests/unit
python -m build
```

联网测试需要真实服务器：

```bash
set ELTDX_RUN_LIVE=1
pytest tests/integration
```

## 参考

感谢 [injoyai/tdx](https://github.com/injoyai/tdx) 的启发。

## 联系

- QQ 群：[点击链接加入群聊](https://qm.qq.com/q/zAjpZsvfzy)
- 邮箱：dapaoxixixi@163.com

## 许可证

MIT
