# 字段手册

这份文档解释常用返回模型的字段含义。按调用方法查参数和返回字段时看 [METHOD_REFERENCE.md](METHOD_REFERENCE.md)。底层协议字段和命令号见 [COMMANDS_7709.md](COMMANDS_7709.md)，历史字段对照见 [FIELD_MIGRATION.md](FIELD_MIGRATION.md)。

## 通用约定

| 约定            | 说明                                   |
| ------------- | ------------------------------------ |
| `exchange`    | 市场前缀，常见为 `sh` / `sz` / `bj`          |
| `code`        | 六位代码，例如 `000001`                     |
| `full_code`   | 完整代码，等于 `exchange + code`            |
| `*_raw`       | 协议原始值或原始片段，保留给排查使用                   |
| `record_hex`  | 单条记录原始十六进制                           |
| `raw_payload` | 当前响应 payload 原始 bytes                |
| `*_milli`     | 毫厘价格，通常 `price = price_milli / 1000` |

## SecurityCode

代码表单条记录。

| 字段                     | 含义                                |
| ---------------------- | --------------------------------- |
| `exchange`             | 市场                                |
| `market_id`            | 市场编号，`0=sz`、`1=sh`、`2=bj`         |
| `code`                 | 六位代码                              |
| `name`                 | 名称                                |
| `multiple`             | 协议价格换算相关倍数                        |
| `decimal`              | 小数位                               |
| `previous_close_price` | 昨收参考价                             |
| `volume_ratio_base`    | 量比相关基础值                           |
| `category`             | 派生品种分类，例如 `a_share`、`index`、`etf` |
| `category_reason`      | 分类命中规则说明                          |
| `board`                | 派生板块，例如主板、创业板、科创板、北交所             |
| `board_reason`         | 板块命中规则说明                          |
| `full_code`            | 完整代码属性                            |

## QuoteSnapshot

批量快照返回单条行情。

| 字段                 | 含义          |
| ------------------ | ----------- |
| `last_price`       | 最新价         |
| `pre_close_price`  | 昨收价         |
| `open_price`       | 今开          |
| `high_price`       | 最高          |
| `low_price`        | 最低          |
| `total_hand`       | 总成交量，单位手    |
| `current_hand`     | 现手          |
| `amount`           | 成交额         |
| `inside_dish`      | 内盘          |
| `outer_disc`       | 外盘          |
| `open_amount_yuan` | 开盘金额，单位元    |
| `buy_levels`       | 买一到买五       |
| `sell_levels`      | 卖一到卖五       |
| `change`           | 涨跌额，派生字段    |
| `change_pct`       | 涨跌幅百分比，派生字段 |
| `sum_buy_vol`      | 五档买量合计，派生字段 |
| `sum_sell_vol`     | 五档卖量合计，派生字段 |

## CategoryQuoteRecord

分类行情列表记录，对应按板块/类别排序拉取。

| 字段                                        | 含义                    |
| ----------------------------------------- | --------------------- |
| `last_price` / `pre_close_price`          | 最新价 / 昨收              |
| `open_price` / `high_price` / `low_price` | 开高低                   |
| `total_hand` / `current_hand`             | 总量 / 现量               |
| `amount`                                  | 成交额                   |
| `inside_dish` / `outer_disc`              | 内外盘                   |
| `bid1` / `ask1`                           | 买一 / 卖一价格             |
| `bid_vol1` / `ask_vol1`                   | 买一 / 卖一量              |
| `rise_speed`                              | 涨速                    |
| `short_turnover`                          | 短周期换手口径字段             |
| `min2_amount`                             | 近 2 分钟金额口径字段          |
| `opening_rush`                            | 开盘冲击口径字段              |
| `vol_rise_speed`                          | 量增速                   |
| `depth`                                   | 深度字段                  |
| `locked_amount`                           | 买一价格 * 买一量 * 100，派生字段 |
| `record_hex`                              | 单条记录原始十六进制            |

## KlineSeries / KlineBar

K 线响应和单根 K 线。

| 字段                                | 含义                        |
| --------------------------------- | ------------------------- |
| `period_name`                     | 周期，例如 `day`、`1m`          |
| `adjust_mode`                     | 复权模式，`none`、`qfq`、`hfq` 等 |
| `anchor_date`                     | 定点复权日期                    |
| `bars`                            | K 线列表                     |
| `time`                            | K 线时间                     |
| `open` / `high` / `low` / `close` | 开高低收                      |
| `last_close_price_milli`          | 上一根收盘毫厘价                  |
| `volume_lots`                     | 成交量，单位手                   |
| `amount`                          | 成交额                       |
| `up_count` / `down_count`         | 上涨/下跌家数，指数类样本可能有          |
| `raw_payload`                     | 响应 payload                |
| `record_hex`                      | 单条 K 线记录原文                |

## MinuteSeries / MinutePoint

分时响应和单点。

| 字段             | 含义           |
| -------------- | ------------ |
| `trading_date` | 交易日          |
| `points`       | 分时点          |
| `prev_close`   | 昨收           |
| `open_price`   | 今开           |
| `index`        | 分时序号         |
| `time_label`   | 时间文本         |
| `price`        | 当前分时价格       |
| `avg_price`    | 均价           |
| `volume`       | 分钟成交量，单位手    |
| `volume_sum`   | 分时成交量合计，派生字段 |

## TradePage / TradeTick

成交明细响应和单笔成交。

| 字段                  | 含义                            |
| ------------------- | ----------------------------- |
| `trading_date`      | 交易日                           |
| `start`             | 请求起始位置                        |
| `request_count`     | 请求条数                          |
| `ticks`             | 成交明细                          |
| `time_label`        | 时间文本                          |
| `trade_datetime`    | 成交时间                          |
| `price`             | 成交价                           |
| `volume`            | 成交量，单位手                       |
| `order_count`       | 单笔包含的订单数                      |
| `side`              | 方向，`buy` / `sell` / `neutral` |
| `trade_amount_yuan` | 成交金额，派生字段                     |
| `has_more`          | 是否可能还有下一页，派生字段                |

## AuctionSeries / AuctionPoint

集合竞价明细。

| 字段                         | 含义         |
| -------------------------- | ---------- |
| `points`                   | 竞价明细记录     |
| `time_label`               | 时间         |
| `price`                    | 竞价价格       |
| `matched_volume`           | 虚拟成交量      |
| `unmatched_volume`         | 未匹配量       |
| `unmatched_direction_raw`  | 未匹配方向原始值   |
| `matched_amount_estimated` | 估算成交额，派生字段 |

## CapitalChangeBlock / CapitalChangeRecord

股本变迁 / 除权相关事件。

| 字段                                                | 含义           |
| ------------------------------------------------- | ------------ |
| `records` / `items`                               | 事件列表         |
| `date`                                            | 事件日期         |
| `category_raw`                                    | 事件类别编号       |
| `category_name`                                   | 类别名称         |
| `c1_value` / `c2_value` / `c3_value` / `c4_value` | 按类别解码后的四个业务值 |
| `c1_raw` / `c2_raw` / `c3_raw` / `c4_raw`         | 四个原始字段       |

## XdxrRecord / EquityRecord

从股本变迁中整理出来的本地派生记录。

| 字段             | 含义   |
| -------------- | ---- |
| `fenhong`      | 分红   |
| `peigujia`     | 配股价  |
| `songzhuangu`  | 送转股  |
| `peigu`        | 配股   |
| `float_shares` | 流通股本 |
| `total_shares` | 总股本  |

## FinanceRecord

财务基础信息。

| 字段                              | 含义          |
| ------------------------------- | ----------- |
| `updated_date`                  | 财务数据更新日期    |
| `ipo_date`                      | 上市日期        |
| `circulating_shares`            | 流通股本，派生字段   |
| `total_shares`                  | 总股本，派生字段    |
| `total_assets_yuan`             | 总资产，派生字段    |
| `net_profit_yuan`               | 净利润，派生字段    |
| `eps_raw`                       | 每股收益原始值     |
| `province_raw` / `industry_raw` | 地区 / 行业原始编号 |

## SpecialLimitRecord

特殊品种涨跌停限制表。

| 字段                   | 含义     |
| -------------------- | ------ |
| `code` / `full_code` | 代码     |
| `limit_up_price`     | 涨停价    |
| `limit_down_price`   | 跌停价    |
| `record_hex`         | 单条记录原文 |

## WorkdayService

交易日工具。

| 方法                       | 含义               |
| ------------------------ | ---------------- |
| `refresh()`              | 用基准指数日 K 加载真实交易日 |
| `is_workday(date)`       | 判断是否交易日          |
| `previous_workday(date)` | 上一个交易日           |
| `next_workday(date)`     | 下一个交易日           |
| `range(start, end)`      | 交易日区间            |
| `today_is_workday()`     | 今天是否交易日          |

如果 `WorkdayService` 绑定了真实客户端，交易日来自基准指数日 K。对于超过当前 K 线范围的未来日期，`next_workday()` 可能返回 `None`。

## Helper 返回模型

### StockProfileTable / StockProfile

股票信息汇总和批量行情表。

| 字段                                                | 含义            |
| ------------------------------------------------- | ------------- |
| `codes`                                           | 请求代码          |
| `rows`                                            | 股票信息行         |
| `full_code` / `name`                              | 完整代码 / 名称     |
| `category` / `board`                              | 品种分类 / 板块分类   |
| `last_price` / `pre_close_price`                  | 最新价 / 昨收      |
| `change` / `change_pct`                           | 涨跌额 / 涨跌幅     |
| `volume_hand` / `amount`                          | 成交量，单位手 / 成交额 |
| `open_amount_yuan`                                | 开盘金额          |
| `circulating_shares` / `total_shares`             | 流通股本 / 总股本    |
| `turnover_rate`                                   | 本地计算换手率       |
| `circulating_market_value` / `total_market_value` | 流通市值 / 总市值    |
| `security` / `quote` / `finance`                  | 合并前的原始模型对象    |

### StockTopics / StockTopic

个股概念板块。

| 字段                             | 含义           |
| ------------------------------ | ------------ |
| `code`                         | 完整股票代码       |
| `topics`                       | 题材列表         |
| `topic_id` / `topic_name`      | 题材 ID / 题材名称 |
| `relation_level`               | 关联度          |
| `selected_date` / `topic_date` | 入选日期 / 题材日期  |
| `reason`                       | 入选原因         |
| `detail_id`                    | 详情记录 ID      |
| `source`                       | 合并来源         |
| `raw`                          | F10 原始行      |

### TopicStockTable / TopicStock

概念板块成分股。

| 字段                                  | 含义                 |
| ----------------------------------- | ------------------ |
| `seed_code`                         | 查询时使用的种子股票         |
| `topic_id` / `topic_name`           | 题材 ID / 题材名称       |
| `sort_by`                           | 排序字段               |
| `rows`                              | 成分股列表              |
| `rank`                              | 题材内排名              |
| `full_code` / `name`                | 完整代码 / 股票简称        |
| `change_pct`                        | 当日涨跌幅              |
| `change_pct_3d` / `change_pct_5d`   | 近 3 日 / 近 5 日涨跌幅   |
| `change_pct_20d` / `change_pct_60d` | 近 20 日 / 近 60 日涨跌幅 |
| `change_pct_ytd`                    | 年初以来涨跌幅            |
| `trading_date`                      | 统计日期               |
| `raw`                               | F10 原始行            |

### AuctionData

竞价组合结果。

| 字段                      | 含义           |
| ----------------------- | ------------ |
| `code` / `trading_date` | 完整代码 / 交易日   |
| `series`                | 集合竞价明细       |
| `snapshot_0925`         | 09:25 竞价成交快照 |
| `pre_close_price`       | 昨收           |
| `open_price`            | 开盘价          |
| `open_volume`           | 09:25 成交量    |
| `open_amount`           | 09:25 成交额    |
| `open_change_pct`       | 开盘涨幅         |

## 缓存口径

默认缓存低频数据：代码数量、全量代码表、股本变迁、财务完整结果。实时快照、分时、成交明细、K 线不缓存。

强制刷新用 `refresh=True`，清空全部缓存用 `client.clear_cache()`。
