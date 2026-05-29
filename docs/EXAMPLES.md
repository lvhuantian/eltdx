# 使用示例

这些示例都基于真实 `7709` 行情主站。代码建议带市场前缀，例如 `sz000001`、`sh600000`、`bj920001`。

## 快照

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    quotes = client.get_quote(["sz000001", "sh600000"])

for item in quotes:
    print(item.full_code, item.last_price, item.change_pct, item.total_hand)
```

## 代码表

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    print(client.get_count("sz"))
    print(client.get_codes("sz", start=0, limit=5))
    print(client.get_a_share_codes_all()[:10])
```

## K 线和复权 K 线

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    day = client.get_kline("day", "sz000001", count=5)
    qfq = client.get_adjusted_kline("day", "sz000001", adjust="qfq", count=5)
    hfq = client.bars.get("sz000001", period="day", adjust="hfq", count=5)

print(day.bars[-1].time, day.bars[-1].close)
print(qfq.adjust_mode, qfq.bars[-1].close)
print(hfq.adjust_mode, hfq.bars[-1].close)
```

## 分时

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    today = client.get_minute("sz000001")
    history = client.get_history_minute("sz000001", "2026-05-20")

print(today.trading_date, today.count)
print(history.trading_date, history.points[-1].price)
```

## 成交明细和 09:25 竞价

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    ticks = client.get_trades("sz000001", count=20)
    auction = client.get_auction_0925("sz000001", "2026-05-20")

print(ticks.count, ticks.ticks[0].time_label, ticks.ticks[0].price)
print(auction.has_auction_0925, auction.price, auction.volume)
```

## 股本、换手率和复权因子

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    gbbq = client.get_gbbq("sz000001")
    equity = client.get_equity("sz000001", "2026-05-20")
    turnover = client.get_turnover("sz000001", 123456, on="2026-05-20", unit="hand")
    factors = client.get_factors("sz000001")

print(gbbq.count)
print(equity.float_shares, equity.total_shares)
print(turnover)
print(factors.count)
```

## 主站测速和连接池

```python
from eltdx import TdxClient

with TdxClient.from_hosts(pool_size=2, probe_hosts=True, timeout=3) as client:
    print(client.transport.hosts[:3])
    print(client.get_quote("sz000001")[0].last_price)
```

## JSON 输出

```python
from eltdx import TdxClient, to_json

with TdxClient(timeout=3) as client:
    quotes = client.get_quote(["sz000001", "sh600000"])

print(to_json(quotes, indent=2))
```

## 常用问题

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    profiles = client.helpers.stock_profile_table(["sz000001", "sh600000"])
    topics = client.helpers.stock_topics("000034")
    stocks = client.helpers.topic_stocks("000034", topic_name="存储芯片")
    auction = client.helpers.auction_data("sz000001", "2026-05-20")

print(profiles.rows[0].name, profiles.rows[0].last_price)
print(topics.topics[:3])
print(stocks.rows[:10])
print(auction.open_price, auction.open_change_pct, auction.open_amount)
```

## 协议排查

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    minute = client.get_minute("sz000001", include_raw=True)

print(minute.raw_payload.hex())
print(minute.points[0].record_hex)
```
