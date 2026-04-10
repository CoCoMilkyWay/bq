# BigTrader 文档整理

## 交易所后缀

平台各交易所代码后缀说明，均为真实交易代码 + 交易所后缀：

| 交易所          | 代码后缀 | 示例                                           | 说明                                                   |
| --------------- | -------- | ---------------------------------------------- | ------------------------------------------------------ |
| 上交所 SSE      | SH       | `600000.SH`、`510050.SH`、`000001.SH`          | 含股票、基金、可转债、指数                             |
| 深交所 SZSE     | SZ       | `000001.SZ`、`159919.SZ`、`399001.SZ`          | 含股票、基金、可转债、指数                             |
| 北交所 BSE      | BJ       | `920099.BJ`                                    | 含股票、指数                                           |
| 中金所 CFFEX    | CFE      | `IF2501.CFE`、`T2503.CFE`、`IO2501-C-4300.CFE` | 含股指期货、国债期货、股指期权                         |
| 上期所 SHFE     | SHF      | `rb2505.SHF`、`cu2503.SHF`                     | 含期货和期货期权，合约代码为小写 + 2 位年份 + 2 位月份 |
| 上能所 INE      | INE      | `sc2505.SHF`                                   | 含期货和期货期权，合约代码为小写 + 2 位年份 + 2 位月份 |
| 大商所 DCE      | DCE      | `a2505.DCE`                                    | 含期货和期货期权，合约代码为小写 + 2 位年份 + 2 位月份 |
| 郑商所 CZCE     | CZC      | `SR505.CZC`                                    | 含期货和期货期权，合约代码为大写 + 1 位年份 + 2 位月份 |
| 广期所 GFEX     | GFE      | `si2505.GFE`                                   | 含期货和期货期权，合约代码为小写 + 2 位年份 + 2 位月份 |
| 上交所期权 SSE  | SHO      | `10000001.SHO`                                 | -                                                      |
| 深交所期权 SZSE | SZO      | `90000001.SZO`                                 | -                                                      |

## 策略回调函数介绍

每个策略需要实现以下一个或多个回调函数，并传给交易引擎。交易引擎会在不同事件触发时调用对应回调，以通知策略事件变化（如行情更新、委托回报通知、成交回报通知）。策略逻辑主要在 `handle_data` 回调函数内实现。

| 函数名称         | 函数英文名称                       | 说明                                                                 |
| ---------------- | ---------------------------------- | -------------------------------------------------------------------- |
| 初始化函数       | `initialize(context)`              | 只触发一次。可初始化变量、读取配置、设置交易费率、设置交易滑点等。   |
| 盘前处理函数     | `before_trading(context, data)`    | 每日盘前触发一次。可处理当日交易准备（如高频回测订阅行情）。         |
| 行情处理函数     | `handle_data(context, data)`       | K 线行情通知函数，支持日线和分钟。多合约时会等待数据到齐后统一触发。 |
| Tick 处理函数    | `handle_tick(context, tick)`       | Tick 快照行情通知函数。每个标的行情变化时触发。                      |
| 逐笔成交处理函数 | `handle_l2trade(context, l2trade)` | 逐笔成交行情更新处理函数。                                           |
| 逐笔委托处理函数 | `handle_l2order(context, l2order)` | 逐笔委托行情更新处理函数。                                           |
| 委托回报通知函数 | `handle_order(context, order)`     | 每个订单状态变化时触发。                                             |
| 成交回报通知函数 | `handle_trade(context, trade)`     | 有成交时触发。                                                       |
| 盘后处理函数     | `after_trading(context, data)`     | 每日盘后运行一次。                                                   |

高频使用函数主要是 `initialize` 与 `handle_data`：

- 事件总数可理解为 K 线事件序列（示例中 26 个事件）。
- `initialize` 只在第一根 K 线（第一个事件）调用一次。
- `handle_data` 在每个 K 线事件都会调用一次。

## 什么是策略的 `context`

`context` 是策略上下文对象。每个策略函数的第一个参数都是 `context`。必须通过该对象获取交易相关资产（资金、持仓、委托等），也必须通过该对象完成下单操作。

### `context` 属性

- `portfolio`：投资组合对象，包含资金 `cash: float`、持仓字典 `positions: dict` 等属性。
- `user_store`：用户持久化变量对象（字典类型），用于模拟交易变量持久化。
- `data`：用户通过回测入口传入的自定义数据（预测/因子等），通过 `context.data` 访问。

### `context` 接口

下单接口示例：

- `context.order(instrument, 100)`
- `context.order_target_percent(instrument, 0.2)`

下单接口公有说明：

- `instrument: str`：平台标的代码，如 `000001.SZ`、`rb2501.SHF`。
- `limit_price: float`：默认为 `None`（市价）。上交所市价单实盘时该字段为保护价，并指定 `order_type=OrderType.MARKET`。
- `order_type: OrderType`：默认为 `None`。指定后按指定类型优先。
- 返回值为 `int`：小于 0 表示失败，可用 `context.get_error_msg(ret_code)` 获取错误信息。
- 下单成功后可通过 `context.get_last_order_key()` 获取本地唯一委托编号。

#### 交易类接口

| 接口名称                                                                                                                                                                                   | 说明                                                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------- |
| `order(instrument: str, order_qty: int, limit_price=None, order_type=None)`                                                                                                                | 普通下单。`order_qty>0` 买，`order_qty<0` 卖。                  |
| `order_target(instrument: str, target: int, limit_price=None, order_type=None)`                                                                                                            | 按目标量下单。期货可为负，表示做空到目标手数。                  |
| `order_target_percent(instrument: str, percent: float, limit_price=None, order_type=None)`                                                                                                 | 按目标仓位下单（按总资产占比）。                                |
| `order_target_value(instrument: str, value: float, limit_price=None, order_type=None)`                                                                                                     | 按目标金额下单。                                                |
| `order_value(instrument: str, value: float, limit_price=None, order_type=None)`                                                                                                            | 按金额下单。                                                    |
| `order_percent(instrument: str, percent: float, limit_price=None, order_type=None)`                                                                                                        | 按仓位下单。                                                    |
| `buy_open(...)` / `sell_close(...)` / `sell_open(...)` / `buy_close(...)`                                                                                                                  | 期货/期权开平仓接口。                                           |
| `margin_trade(...)` / `margincash_open(...)` / `margincash_close(...)` / `marginsec_open(...)` / `marginsec_close(...)` / `margincash_direct_refund(...)` / `marginsec_direct_refund(...)` | 两融相关接口。                                                  |
| `context.cancel_order(order_param)`                                                                                                                                                        | 撤单，`order_param` 可传 `order`、`order_key` 或 `cancel_req`。 |

#### 查询类接口

| 接口名称                                                        | 说明                                                                        |
| --------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `get_trading_account() -> FundData`                             | 资金账户（总资金、可用资金、保证金、持仓市值等）。                          |
| `get_marginasset_data() -> Dict`                                | 两融负债信息（融资/融券负债等）。                                           |
| `get_balance() -> float`                                        | 总资金 = 可用资金 + 冻结资金。                                              |
| `get_available_cash() -> float`                                 | 可用资金。                                                                  |
| `get_portfolio_value() -> float`                                | 账户总资产。股票：总资金 + 总持仓市值；期货：总资金 + 总保证金 + 持仓盈亏。 |
| `get_position(instrument: str, direction=None) -> PositionData` | 单个持仓。期货 `direction='1'` 多头，`'2'` 空头；不传时返回多空组合对象。   |
| `get_positions() -> Dict[key, PositionData]`                    | 所有持仓。期货持仓值为多空组合对象。                                        |
| `get_orders(instrument="") -> List[OrderData]`                  | 当日委托。                                                                  |
| `get_open_orders(instrument="") -> List[OrderData]`             | 未成交委托。                                                                |
| `get_trades(instrument="") -> List[TradeData]`                  | 当日成交。                                                                  |
| `get_last_price(instrument: str) -> float`                      | 最新价格。                                                                  |
| `get_contract(instrument: str) -> ContractData`                 | 合约信息（`multiplier`、`price_tick`、`name`、`underlying` 等）。           |
| `get_trading_day() -> str`                                      | 当前交易日，格式 `YYYYmmdd`（夜盘归属次日）。                               |

#### 设置类接口

```python
from bigtrader.finance.commission import PerOrder, PerContract

set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0003, min_cost=5, tax_ratio=0.0005))
set_commission(futures_commission=PerContract(cost={"rb": (2, 2, 1), "IF": (0.000023, 0.00015, 0.000023)}))
set_commission_ratio(code: str, open_ratio: float, close_ratio: float, closetoday_ratio: float)
set_margin_ratio(code: str, margin_ratio: float)
set_slippage_name("fixed")
set_slippage_value(slippage_type=1, slippage_value=1.0)
set_slippage_value(slippage_type=2, slippage_value=0.005)
```

- 股票费率用 `PerOrder`，期货费率用 `PerContract` 或 `set_commission_ratio`。
- `set_margin_ratio` 为期货保证金设置。
- `slippage_type=1` 为固定滑点，`slippage_type=2` 为百分比滑点。

#### 其它接口

- 订阅 Tick：`subscribe(instruments: List[str] | str)`
- 订阅分钟：`subscribe_bar(instruments: List[str] | str)`
- 取消订阅：`unsubscribe(instruments: List[str] | str)`
- 回测模式判断：`is_backtest_mode() -> bool`

## 什么是 `data`

多个策略回调函数的第二个参数是 `data`（`IBarData` 对象），封装了访问 Bar 数据的接口。

### `data` 属性

| 属性名称              | 属性说明                                       |
| --------------------- | ---------------------------------------------- |
| `data.current_dt`     | `datetime`，当前回测自然日期 + 时间。          |
| `data.trading_day_dt` | `datetime`，当前交易日日期（夜盘可能为次日）。 |

### `data` 接口

| 接口名称                                          | 接口说明                                               |
| ------------------------------------------------- | ------------------------------------------------------ |
| `current(instrument: str, field: str) -> float    | int`                                                   | 获取某根 Bar 的字段值（如 `open`、`close`、`volume`）。 |
| `history(instrument: str, fields: List[str]       | str, count: int, frequency: str, expect_ndarray=True)` | 获取历史 N 根 Bar；`frequency` 为 `1d`/`1m`。           |
| `get_daily_value(instrument, field: str) -> float | int`                                                   | 获取日 K 线字段值。                                     |

## BigTrader 其它说明

### 除权除息处理

平台使用复权因子处理：若当天与前一天复权因子变化，则按变化比率调整持仓数量与持仓价格，并生成对应成交记录（转送股数量或分红金额等）。

### 退市/到期处理

- 股票退市：按最新价自动平仓，成交时间 `00:00:00`，并记录 `expire` 日志。
- 期货到期：按最新价自动平仓，释放保证金与平仓盈亏，成交时间 `00:00:00`，并记录 `expire` 日志。

### 撮合逻辑

- 日频：买入 `> 最低价`、卖出 `< 最高价` 则成交；成交价取决于回测配置的 `open/close`。
- 分钟：参考下一分钟 `open`，可持续撮合至收盘或订单成交完成。
- Tick：盘后撮合，按盘口档位逐档成交。

### 日频下单说明

日频在 `handle_data` 下单，会在下一交易日进入撮合。

### 高频订阅说明

高频回测/实盘都需要在盘前订阅当天需要的代码行情，避免无关订阅带来性能损耗。

### 回测结果明细与性能建议

- 回测结束会返回 `raw_perf`（`DataFrame`），包含每日信号、持仓、委托、成交。
- 尽量把当日静态数据准备放在 `before_trading`，减少 `handle_data/handle_tick` 中频繁创建 `pandas` 对象，优先 `numpy`。

### 变量持久化

- 模拟交易每天会触发 `initialize()`，初始化前应先判断变量是否已在 `context.user_store`。
- 可使用 `context.user_store.init_once(k1=v1, k2=v2)`。

## BigTrader 的数据对象

### 资金数据 `FundData(TradingAccount)`

| 属性                 | 含义           | 补充说明                                                  |
| -------------------- | -------------- | --------------------------------------------------------- |
| `account_id`         | 资金账号       | -                                                         |
| `balance`            | 总资金         | 可用资金 + 冻结资金                                       |
| `available`          | 可用资金       | -                                                         |
| `frozen_cash`        | 冻结资金       | -                                                         |
| `portfolio_value`    | 总资产         | 股票：总资金 + 总市值；期货：总资金 + 总保证金 + 持仓盈亏 |
| `total_market_value` | 总市值         | 股票/基金/债券/期权                                       |
| `total_margin`       | 总保证金       | 期货/期权卖方                                             |
| `commission`         | 当日总交易费用 | -                                                         |
| `positions_pnl`      | 持仓盈亏       | -                                                         |

### 持仓数据 `PositionData`

| 属性             | 含义       | 补充说明               |
| ---------------- | ---------- | ---------------------- |
| `instrument`     | 持仓代码   | -                      |
| `posi_direction` | 持仓方向   | `'1'` 多头，`'2'` 空头 |
| `current_qty`    | 持仓数量   | -                      |
| `avail_qty`      | 可用数量   | -                      |
| `cost_price`     | 持仓均价   | -                      |
| `today_qty`      | 今仓数量   | -                      |
| `margin`         | 保证金占用 | -                      |
| `last_price`     | 最新价     | -                      |
| `open_date`      | 开仓日期   | `YYYYmmdd`             |
| `open_price`     | 开仓均价   | -                      |

### 委托数据 `OrderData`

| 属性           | 含义         | 补充说明                            |
| -------------- | ------------ | ----------------------------------- |
| `instrument`   | 持仓代码     | -                                   |
| `direction`    | 持仓方向     | `Direction.BUY / SELL`              |
| `offset_flag`  | 开平标志     | `OffsetFlag.OPEN/CLOSE/CLOSETODAY`  |
| `order_type`   | 委托类型     | `OrderType.LIMIT/MARKET/MARKET_FOK` |
| `order_qty`    | 委托数量     | -                                   |
| `filled_qty`   | 成交数量     | -                                   |
| `order_price`  | 委托均价     | -                                   |
| `order_status` | 委托状态     | 见下方 `OrderStatus`                |
| `order_sysid`  | 系统报单编号 | -                                   |
| `order_key`    | 本地唯一单号 | -                                   |
| `user_id`      | 操作员代码   | -                                   |
| `insert_date`  | 报单日期     | `YYYYmmdd`                          |
| `order_time`   | 报单时间     | `HHMMSSmmm`                         |
| `trading_day`  | 交易日       | `YYYYmmdd`                          |
| `status_msg`   | 报单状态消息 | -                                   |

### 成交数据 `TradeData`

| 属性           | 含义         | 补充说明                                                                                   |
| -------------- | ------------ | ------------------------------------------------------------------------------------------ |
| `instrument`   | 持仓代码     | -                                                                                          |
| `direction`    | 持仓方向     | `Direction.BUY / SELL`                                                                     |
| `offset_flag`  | 开平标志     | `OffsetFlag.OPEN/CLOSE/CLOSETODAY`                                                         |
| `trade_type`   | 成交类型     | `'0'` 普通成交，`'1'` 行权，`'D'` 除权除息，`'E'` 退市平仓，`'F'` 强平，`'R'` 直接还款还券 |
| `filled_qty`   | 成交数量     | -                                                                                          |
| `filled_price` | 成交价格     | -                                                                                          |
| `filled_money` | 成交金额     | -                                                                                          |
| `trade_id`     | 成交编号     | -                                                                                          |
| `order_sysid`  | 系统报单编号 | -                                                                                          |
| `order_key`    | 本地唯一单号 | -                                                                                          |
| `user_id`      | 操作员代码   | -                                                                                          |
| `trade_date`   | 成交日期     | `YYYYmmdd`                                                                                 |
| `trade_time`   | 成交时间     | `HHMMSSmmm`                                                                                |
| `trading_day`  | 交易日       | `YYYYmmdd`                                                                                 |

### Tick 数据

股票 Level1 5 档（Level2 10 档，约 3 秒更新）；期货 Level1 1 档（约 500ms 更新，Level2 5 档，部分交易所 250ms）。

| 属性                                            | 含义                  | 补充说明    |
| ----------------------------------------------- | --------------------- | ----------- |
| `instrument`                                    | 持仓代码              | -           |
| `datetime`                                      | tick 日期时间         | -           |
| `time`                                          | tick 时间             | `HHMMSSmmm` |
| `last_price/open_price/high_price/low_price`    | 最新/开盘/最高/最低价 | -           |
| `volume/amount`                                 | 成交量/成交额         | -           |
| `open_interest/pre_open_interest`               | 持仓量/昨持仓量       | -           |
| `pre_close`                                     | 昨收盘                | -           |
| `upper_limit/lower_limit`                       | 涨停/跌停价           | -           |
| `deal_number`                                   | 股票成交笔数          | -           |
| `iopv`                                          | 基金估值              | -           |
| `ask_volumeX/ask_priceX/bid_priceX/bid_volumeX` | 卖量/卖价/买价/买量   | `X: 1~10`   |

## BigTrader 的数据字典

- 买卖方向：`Direction.BUY='1'`，`SELL='2'`，`FinancingBuy='3'`，`SellRepay='4'`，`LoanSell='5'`，`BuyRedeliver='6'`
- 开平标志：`OffsetFlag.OPEN='0'`，`CLOSE='1'`，`CLOSE_TODAY='2'`，`CLOSE_YESTERDAY='3'`
- 委托类型：`OrderType.LIMIT='0'`，`MARKET='U'`

### 委托状态 `OrderStatus`

| 状态                    | 值  | 说明     |
| ----------------------- | --- | -------- |
| `OrderStatus.NOTTRADED` | 0   | 未成交   |
| `PARTTRADED`            | 1   | 部分成交 |
| `ALLTRADED`             | 2   | 全部成交 |
| `PARTCANCELLED`         | 3   | 部分撤单 |
| `CANCELLED`             | 4   | 全部撤单 |
| `REJECTED`              | 5   | 废单     |
| `UNKNOWN`               | 6   | 未知     |
| `NOTPLACE`              | 10  | 未报     |
| `PLACING`               | 11  | 正报     |
| `PENDINGPLACE`          | 12  | 待报     |
| `PARTPENDINGPLACE`      | 15  | 部分待撤 |
| `PENDINGPLACE`          | 16  | 待撤销   |

## 撮合与策略运行结果说明

### 订单撮合处理

基于历史行情模拟撮合，支持 Bar、快照、逐笔三类数据。支持上交所/深交所上市 A 股股票、基金、债券、期权、期货等品种。不支持新股申购、市值配售、增发申购、配股等交易。默认股票费用：买入 `0.03%`、卖出 `0.13%`（含默认 `0.1%` 印花税），期货按品种费率计算。

### 通用规则

- 未成交或部分成交委托可撤单。
- 当天未成交委托收市后自动作废，不参与下一交易日撮合。
- 市价单未成交部分自动撤销（即成剩撤）。

### Bar 撮合

- 可指定 `open/close` 作为成交参考价。
- 分钟回测一般按下单后第一分钟开盘价或后续限价撮合。
- 成交量上限受当次 Bar 成交量（及成交率）约束。

### 快照撮合

- 基于最新价撮合，发单后用下一笔快照撮合。
- 买入：最新价 `<=` 委托价可成交；涨停/买一排队时进入等待队列。
- 卖出：最新价 `>=` 委托价可成交；跌停/卖一排队时进入等待队列。
- 集合竞价报单通常在 `09:25:00` 或 `09:30:00` 后连续竞价撮合。

### 逐笔撮合

- 基于逐笔最新成交价，发单后用下一笔逐笔成交撮合，并结合快照盘口。
- 市价单在涨跌停可能不成交并撤销；可成交时按盘口档位撮合。
- 文档原有 `FIXME` 场景（超十档、排队量更逼真建模）保留。

## 回测结果分析

回测结果图主要包含：收益概况、交易详情、每日持仓及收益、输出日志。

### 收益概况

黄色为策略收益率，蓝色为沪深 300 基准收益率，绿色为持仓占比（仓位）。可通过图例显示相对收益率曲线。

### 常见指标解释

- 收益率、年化收益率、基准收益率
- 阿尔法、贝塔、夏普比率
- 胜率、盈亏比、收益波动率、最大回撤、信息比率

关于指标详解可参考：`策略回测结果指标详解`。
# BigTrader 文档整理

## 交易所后缀

平台各交易所代码后缀说明，均为真实交易代码 + 交易所后缀：

| 交易所          | 代码后缀 | 示例                                           | 说明                                                   |
| --------------- | -------- | ---------------------------------------------- | ------------------------------------------------------ |
| 上交所 SSE      | SH       | `600000.SH`、`510050.SH`、`000001.SH`          | 含股票、基金、可转债、指数                             |
| 深交所 SZSE     | SZ       | `000001.SZ`、`159919.SZ`、`399001.SZ`          | 含股票、基金、可转债、指数                             |
| 北交所 BSE      | BJ       | `920099.BJ`                                    | 含股票、指数                                           |
| 中金所 CFFEX    | CFE      | `IF2501.CFE`、`T2503.CFE`、`IO2501-C-4300.CFE` | 含股指期货、国债期货、股指期权                         |
| 上期所 SHFE     | SHF      | `rb2505.SHF`、`cu2503.SHF`                     | 含期货和期货期权，合约代码为小写 + 2 位年份 + 2 位月份 |
| 上能所 INE      | INE      | `sc2505.SHF`                                   | 含期货和期货期权，合约代码为小写 + 2 位年份 + 2 位月份 |
| 大商所 DCE      | DCE      | `a2505.DCE`                                    | 含期货和期货期权，合约代码为小写 + 2 位年份 + 2 位月份 |
| 郑商所 CZCE     | CZC      | `SR505.CZC`                                    | 含期货和期货期权，合约代码为大写 + 1 位年份 + 2 位月份 |
| 广期所 GFEX     | GFE      | `si2505.GFE`                                   | 含期货和期货期权，合约代码为小写 + 2 位年份 + 2 位月份 |
| 上交所期权 SSE  | SHO      | `10000001.SHO`                                 | -                                                      |
| 深交所期权 SZSE | SZO      | `90000001.SZO`                                 | -                                                      |

## 策略回调函数介绍

每个策略需要实现以下一个或多个回调函数，并传给交易引擎。交易引擎会在不同事件触发时调用对应回调，以通知策略事件变化（如行情更新、委托回报通知、成交回报通知）。策略逻辑主要在 `handle_data` 回调函数内实现。

| 函数名称         | 函数英文名称                       | 说明                                                                                     |
| ---------------- | ---------------------------------- | ---------------------------------------------------------------------------------------- |
| 初始化函数       | `initialize(context)`              | 策略初始化函数，只触发一次。可初始化变量、读取配置、设置交易费率、设置交易滑点等。       |
| 盘前处理函数     | `before_trading(context, data)`    | 每日盘前触发一次。可处理当日交易前准备（如高频回测时订阅行情）。                         |
| 行情处理函数     | `handle_data(context, data)`       | K 线行情通知函数，支持日线和分钟。注册多个合约时，会等待所有合约数据到齐后统一触发一次。 |
| Tick 处理函数    | `handle_tick(context, tick)`       | Tick 快照行情通知函数。每个标的行情变化时触发，依赖交易所实时行情推送。                  |
| 逐笔成交处理函数 | `handle_l2trade(context, l2trade)` | 逐笔成交行情更新处理函数。                                                               |
| 逐笔委托处理函数 | `handle_l2order(context, l2order)` | 逐笔委托行情更新处理函数。                                                               |
| 委托回报通知函数 | `handle_order(context, order)`     | 每个订单状态变化时触发。                                                                 |
| 成交回报通知函数 | `handle_trade(context, trade)`     | 有成交时触发。                                                                           |
| 盘后处理函数     | `after_trading(context, data)`     | 每日盘后运行一次。                                                                       |

高频使用函数主要是 `initialize` 与 `handle_data`：

- 一共有 26 个事件（26 根 K 线），第一根 K 线既对应黑色箭头又对应灰色箭头，其余只对应灰色箭头。
- `initialize` 只在第一个事件（第一根 K 线）调用一次，适合放初始化设置。
- 每个 K 线事件都会调用一次 `handle_data`，适合放主要策略逻辑。

## 什么是策略的 `context`

`context` 是策略上下文对象。每个策略函数的第一个参数都是 `context`。必须通过该对象获取交易相关资产（资金、持仓、委托等），也必须通过该对象完成下单操作。

### `context` 有哪些属性

- `portfolio`：投资组合对象，包含资金 `cash: float`、持仓字典 `positions: dict` 等属性。
- `user_store`：用户持久化变量对象（字典类型），主要用于模拟交易，可将策略需要持久化的变量写入该字典。
- `data`：用户通过回测入口传入的自定义数据（如预测/因子数据），在策略中通过 `context.data` 访问。

### `context` 有哪些接口

下单接口调用示例：

- `context.order(instrument, 100)`
- `context.order_target_percent(instrument, 0.2)`

下单接口公有说明：

- `instrument: str`：平台内标的代码，如 `000001.SZ` 或 `rb2501.SHF`。
- `limit_price: float`：默认为 `None`，表示市价单。实盘中上交所市价单时，该字段为保护价，并指定 `order_type=OrderType.MARKET`（市价五档即成剩撤）。
- `order_type: OrderType`：默认为 `None`。指定后以指定类型优先，可从 `bigtrader.constant` 中 import。
- 返回值为 `int`：小于 0 表示失败，失败时可用 `context.get_error_msg(ret_code)` 获取错误信息。
- 下单成功后可通过 `context.get_last_order_key()` 获取刚下单委托的本地唯一编号，后续委托回报可通过 `order_key` 找到对应关系。

#### 交易类接口

| 接口名称                                                                                   | 接口说明                                                                                                |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| `order(instrument: str, order_qty: int, limit_price=None, order_type=None)`                | 普通下单。`order_qty`：`>0` 为买，`<0` 为卖。`limit_price` 为 `None` 时默认市价单，指定价格时为限价单。 |
| `order_target(instrument: str, target: int, limit_price=None, order_type=None)`            | 按目标量下单。`target` 为目标持仓量；期货可为负，表示做空到 `target` 手。                               |
| `order_target_percent(instrument: str, percent: float, limit_price=None, order_type=None)` | 按目标仓位下单。`percent` 为目标持仓占比（按账户总资产计算）。                                          |
| `order_target_value(instrument: str, value: float, limit_price=None, order_type=None)`     | 按目标金额下单。                                                                                        |
| `order_value(instrument: str, value: float, limit_price=None, order_type=None)`            | 按金额下单。                                                                                            |
| `order_percent(instrument: str, percent: float, limit_price=None, order_type=None)`        | 按仓位下单。                                                                                            |
| `buy_open(instrument: str, order_qty: int, limit_price=None, order_type=None)`             | 买入开仓（期货/期权专用）。                                                                             |
| `sell_close(instrument: str, order_qty: int, limit_price=None, order_type=None)`           | 卖出平仓（期货/期权专用）。                                                                             |
| `sell_open(instrument: str, order_qty: int, limit_price=None, order_type=None)`            | 卖出开仓（期货/期权专用）。                                                                             |
| `buy_close(instrument: str, order_qty: int, limit_price=None, order_type=None)`            | 买入平仓（期货/期权专用）。                                                                             |
| `margin_trade(instrument: str, order_qty: int, limit_price=None, order_type=None)`         | 担保品买卖（两融专用）。                                                                                |
| `margincash_open(instrument: str, order_qty: int, limit_price=None, order_type=None)`      | 融资买入（两融专用）。                                                                                  |
| `margincash_close(instrument: str, order_qty: int, limit_price=None, order_type=None)`     | 卖券还款（两融专用）。                                                                                  |
| `marginsec_open(instrument: str, order_qty: int, limit_price=None, order_type=None)`       | 融券卖出（两融专用）。                                                                                  |
| `marginsec_close(instrument: str, order_qty: int, limit_price=None, order_type=None)`      | 买券还券（两融专用）。                                                                                  |
| `margincash_direct_refund(value: float)`                                                   | 直接还款（两融专用）。                                                                                  |
| `marginsec_direct_refund(instrument: str, order_qty: int)`                                 | 直接还券（两融专用）。                                                                                  |
| `context.cancel_order(order_param)`                                                        | 撤单。`order_param` 可传 `order` 数据、`order_key` 或 `cancel_req`。                                    |

#### 查询类接口

| 接口名称                                                        | 接口说明                                                                                                                 |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `get_trading_account() -> FundData`                             | 获取资金账户，包含总资金、可用资金、保证金、持仓市值等。                                                                 |
| `get_marginasset_data() -> Dict`                                | 获取两融负债信息，包含总资金、可用保证金、融资负债、融券负债等。                                                         |
| `get_balance() -> float`                                        | 获取总资金（总资金 = 可用资金 + 冻结资金）。                                                                             |
| `get_available_cash() -> float`                                 | 获取可用资金。                                                                                                           |
| `get_portfolio_value() -> float`                                | 获取账户总资产（股票总资产 = 总资金 + 总持仓市值；期货总资产 = 总资金 + 总保证金 + 持仓盈亏）。                          |
| `get_position(instrument: str, direction=None) -> PositionData` | 获取单个持仓。期货传 `direction='1'` 获取多头，`'2'` 获取空头；不传时返回多空组合对象（可通过 `.long`、`.short` 访问）。 |
| `get_positions() -> Dict[key, PositionData]`                    | 获取所有持仓。若值为期货持仓，返回多空组合对象（可通过 `.long_position()` 或 `.short_position()` 获取）。                |
| `get_orders(instrument="") -> List[OrderData]`                  | 获取当日委托；传 `instrument` 时仅返回该代码委托。                                                                       |
| `get_open_orders(instrument="") -> List[OrderData]`             | 获取未成交委托；传 `instrument` 时仅返回该代码挂单。                                                                     |
| `get_trades(instrument="") -> List[TradeData]`                  | 获取当日成交；传 `instrument` 时仅返回该代码成交。                                                                       |
| `get_last_price(instrument: str) -> float`                      | 获取最新价格。                                                                                                           |
| `get_contract(instrument: str) -> ContractData`                 | 获取合约信息。`ContractData` 主要属性：`multiplier`、`price_tick`、`name`、`underlying`。                                |
| `get_trading_day() -> str`                                      | 获取当前交易日，返回 `YYYYmmdd`；夜盘时交易日为第二天（周末/节假日顺延）。                                               |

#### 设置类接口

设置费率：

```python
from bigtrader.finance.commission import PerOrder, PerContract

set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0003, min_cost=5, tax_ratio=0.0005))
set_commission(futures_commission=PerContract(cost={"rb": (2, 2, 1), "IF": (0.000023, 0.00015, 0.000023)}))
set_commission_ratio(code: str, open_ratio: float, close_ratio: float, closetoday_ratio: float)
```

- 股票使用 `PerOrder` 设置费率：买入费率 `buy_cost`、卖出费率 `sell_cost`、最少费用 `min_cost`、印花税率 `tax_ratio`。
- 期货使用 `PerContract` 设置费率：字典 Key 为品种代码，值为 `(开仓费率, 平仓费率, 平今费率)`。
- 期货也可用 `set_commission_ratio`：`code` 可为品种代码或具体合约代码；费率值 `>=0.1` 时按手数收取，否则按金额比例收取。

设置保证金率：

- `set_margin_ratio(code: str, margin_ratio: float)`
- 期货专用，`code` 可为品种代码（如 `rb`）或完整合约代码（如 `rb2501.SHF`）。

设置滑点：

- `set_slippage_name("fixed")`：设置为策略指定价格成交，一般用于交割单回测。
- `set_slippage_value(slippage_type=1, slippage_value=1.0)` 或 `set_slippage_value(slippage_type=2, slippage_value=0.005)`。
- `slippage_type=1` 表示固定滑点值，`slippage_type=2` 表示滑点百分比值。

#### 其它接口

- 订阅 Tick 数据：`subscribe(instruments: List[str] | str)`
- 订阅分钟数据：`subscribe_bar(instruments: List[str] | str)`
- 取消订阅：`unsubscribe(instruments: List[str] | str)`
- 主要用于高频数据（分钟/Tick）订阅。高频回测时，每日在盘前函数里必须订阅当天需要的代码列表；不需要的代码不要订阅，否则会影响性能。
- 是否是回测模式：`is_backtest_mode() -> bool`

## 什么是 `data`

多个策略回调函数的第二个参数就是 `data`，它是一个 `IBarData` 对象，封装了访问 Bar 数据的接口。

### `data` 有哪些属性

| 属性名称              | 属性说明                                                                                         |
| --------------------- | ------------------------------------------------------------------------------------------------ |
| `data.current_dt`     | `datetime` 类型，表示回测中当前自然日期 + 时间。                                                 |
| `data.trading_day_dt` | `datetime` 类型，表示当前交易日。期货夜盘时，`current_dt` 是夜盘自然日期，交易日则是第二天日期。 |

### `data` 有哪些接口

| 接口名称                                          | 接口说明                                               |
| ------------------------------------------------- | ------------------------------------------------------ |
| `current(instrument: str, field: str) -> float    | int`                                                   | 获取 Bar 某字段值。`instrument` 为标的代码；`field` 为 K 线字段（如 `open`、`close`、`volume`）。     |
| `history(instrument: str, fields: List[str]       | str, count: int, frequency: str, expect_ndarray=True)` | 获取历史 N 根 Bar 值。`frequency` 为 `1d` 或 `1m`；`expect_ndarray=True` 时会尝试返回 `numpy array`。 |
| `get_daily_value(instrument, field: str) -> float | int`                                                   | 获取日 K 线字段值。`instrument` 为标的代码；`field` 为 K 线字段（如 `open`、`close`、`volume`）。     |

## BigTrader 其它说明

### 股票/基金回测/模拟交易如何处理除权除息

平台使用复权因子处理：若当天与前一天复权因子变化，则计算变化比率并调整持仓数量与持仓价格。发生除权除息后，交易引擎会生成一条成交记录，记录转送股数量或分红金额等。

### 股票退市或期货合约到期处理

- 股票：在退市日期按最新价自动平仓，生成平仓成交记录，成交金额返还到账户资金；成交时间为 `00:00:00`，并生成 `expire` 开头日志。
- 期货：在到期日期按最新价自动平仓，释放保证金与平仓盈亏返还到账户资金；成交时间为 `00:00:00`，并生成 `expire` 开头日志。

### 交易引擎撮合逻辑

- 日频撮合：买入时 `> 最低价`、卖出时 `< 最高价` 则成交；成交参考价格取决于回测模块指定的 `open/close`；成交数量 `= min(委托数量, Bar成交量 * volume_limit)`。
- 分钟撮合：买入时 `> 最低价`、卖出时 `< 最高价` 则成交；成交参考价为下一分钟 `open`；成交数量会依次撮合至收盘或订单全部成交。
- Tick 撮合：盘后撮合；买入时 `>= 卖一价`、卖出时 `<= 买一价` 则成交；成交价依次从 1 档到 5 档直至成交完。

### 日频回测下单时机

日频回测在 `handle_data` 中下单，会在下一日撮合成交。因为日频 `handle_data` 表示当日已收盘，策略已收到当日完整 K 线数据，当天无法再交易，订单请求会暂存到下一天发送撮合。

### 未成交委托处理

回测中，当天未成交委托会在盘后自动清空，不会移到下一日继续撮合。注意：日频回测当日新下的订单不属于“未成交委托”，因为订单尚未发送到模拟撮合引擎。

### 高频回测为何每天都要在 `before_trading_start` 订阅行情

- 回测：高频数据存储于 dai 数据平台，数据量极大。回放行情模块需提前知道“当天回放哪些代码 + 频率”，并按时间顺序模拟实盘行情推送。
- 实盘：实时行情服务器在远端，策略需先订阅关心代码列表，才能收到实时行情主推；例如收到一笔 Tick 主推后，交易引擎会立即回调 `handle_tick(context, tick)`。

### 如何自定义分析回测详细交易结果

回测完成时会返回 `raw_perf`（`DataFrame` 类型），其中包含每日详细交易信号、每日持仓、每日委托、每日成交。

### 如何提升策略运行效率

- 当日静态不变数据尽量在 `before_trading` 中提前准备。
- 回测过程中尽量不要在 `handle_data` / `handle_tick` 中频繁创建 `pd.Series` / `pd.DataFrame`，这类操作耗时较高，建议优先使用 `numpy`。

### 策略变量持久化注意事项

- 变量通常在 `initialize()` 中初始化，但模拟交易每天运行都会再次触发 `initialize()`。
- 初始化变量前应先判断变量名是否在 `context.user_store` 中，不存在再设置初值，否则可能导致回测与模拟交易触发信号日期不一致。
- 也可在 `initialize()` 中使用 `context.user_store.init_once(k1=v1, k2=v2)`，内部会判断是否已存在：不存在则设初值，存在则略过。

## BigTrader 的数据对象

### 资金数据 `FundData(TradingAccount)`

| 资金账户属性                | 属性含义       | 补充说明                                                    |
| --------------------------- | -------------- | ----------------------------------------------------------- |
| `account_id: str`           | 资金账号       | -                                                           |
| `balance: float`            | 总资金         | 可用资金 + 冻结资金                                         |
| `available: float`          | 可用资金       | -                                                           |
| `frozen_cash: float`        | 冻结资金       | -                                                           |
| `portfolio_value: float`    | 总资产         | 股票 = 总资金 + 总市值；期货 = 总资金 + 总保证金 + 持仓盈亏 |
| `total_market_value: float` | 总市值         | 股票/基金/债券/期权                                         |
| `total_margin: float`       | 总保证金       | 期货/期权卖方                                               |
| `commission: float`         | 当日总交易费用 | -                                                           |
| `positions_pnl: float`      | 持仓盈亏       | -                                                           |

### 持仓数据 `PositionData`

| 持仓属性              | 属性含义   | 补充说明               |
| --------------------- | ---------- | ---------------------- |
| `instrument: str`     | 持仓代码   | -                      |
| `posi_direction: str` | 持仓方向   | `'1'` 多头，`'2'` 空头 |
| `current_qty: int`    | 持仓数量   | -                      |
| `avail_qty: int`      | 可用数量   | -                      |
| `cost_price: float`   | 持仓均价   | -                      |
| `today_qty: int`      | 今仓数量   | -                      |
| `margin: float`       | 保证金占用 | -                      |
| `last_price: float`   | 最新价     | -                      |
| `open_date: int`      | 开仓日期   | `YYYYmmdd`             |
| `open_price: float`   | 开仓均价   | -                      |

### 委托数据 `OrderData`

| 委托属性                    | 属性含义     | 补充说明                                    |
| --------------------------- | ------------ | ------------------------------------------- |
| `instrument: str`           | 持仓代码     | -                                           |
| `direction: Direction`      | 持仓方向     | `Direction.BUY` / `Direction.SELL`          |
| `offset_flag: OffsetFlag`   | 开平标志     | `OffsetFlag.OPEN` / `CLOSE` / `CLOSETODAY`  |
| `order_type: OrderType`     | 委托类型     | `OrderType.LIMIT` / `MARKET` / `MARKET_FOK` |
| `order_qty: int`            | 委托数量     | -                                           |
| `filled_qty: int`           | 成交数量     | -                                           |
| `order_price: float`        | 委托均价     | -                                           |
| `order_status: OrderStatus` | 委托状态     | 见下文 `OrderStatus` 字典                   |
| `order_sysid: str`          | 系统报单编号 | -                                           |
| `order_key: str`            | 本地唯一单号 | -                                           |
| `user_id: str`              | 操作员代码   | -                                           |
| `insert_date: int`          | 报单日期     | `YYYYmmdd`                                  |
| `order_time: int`           | 报单时间     | `HHMMSSmmm`                                 |
| `trading_day: int`          | 交易日       | `YYYYmmdd`                                  |
| `status_msg: str`           | 报单状态消息 | -                                           |

### 成交数据 `TradeData`

| 成交属性                  | 属性含义     | 补充说明                                                                                           |
| ------------------------- | ------------ | -------------------------------------------------------------------------------------------------- |
| `instrument: str`         | 持仓代码     | -                                                                                                  |
| `direction: Direction`    | 持仓方向     | `Direction.BUY` / `Direction.SELL`                                                                 |
| `offset_flag: OffsetFlag` | 开平标志     | `OffsetFlag.OPEN` / `CLOSE` / `CLOSETODAY`                                                         |
| `trade_type: TradeType`   | 成交类型     | `'0'` 普通成交，`'1'` 期权行权成交，`'D'` 除权除息，`'E'` 退市平仓，`'F'` 强平，`'R'` 直接还款还券 |
| `filled_qty: int`         | 成交数量     | -                                                                                                  |
| `filled_price: float`     | 成交价格     | -                                                                                                  |
| `filled_money: float`     | 成交金额     | -                                                                                                  |
| `trade_id: str`           | 成交编号     | -                                                                                                  |
| `order_sysid: str`        | 系统报单编号 | -                                                                                                  |
| `order_key: str`          | 本地唯一单号 | -                                                                                                  |
| `user_id: str`            | 操作员代码   | -                                                                                                  |
| `trade_date: int`         | 成交日期     | `YYYYmmdd`                                                                                         |
| `trade_time: int`         | 成交时间     | `HHMMSSmmm`                                                                                        |
| `trading_day: int`        | 交易日       | `YYYYmmdd`                                                                                         |

### Tick 数据

股票 Level1 行情只有 5 档（Level2 为 10 档，均 3 秒更新一次），期货 Level1 行情只有 1 档（500ms 更新一次，Level2 为 5 档，部分交易所 250ms 更新一次）。

| Tick 属性                | 属性含义      | 补充说明    |
| ------------------------ | ------------- | ----------- |
| `instrument: str`        | 持仓代码      | -           |
| `datetime: datetime`     | tick 日期时间 | -           |
| `time: int`              | tick 时间     | `HHMMSSmmm` |
| `last_price: float`      | 最新价格      | -           |
| `open_price: float`      | 开盘价格      | -           |
| `high_price: float`      | 最高价格      | -           |
| `low_price: float`       | 最低价格      | -           |
| `volume: int`            | 成交量        | -           |
| `amount: float`          | 成交额        | -           |
| `open_interest: int`     | 持仓量        | -           |
| `pre_open_interest: int` | 昨持仓量      | -           |
| `pre_close: float`       | 昨收盘        | -           |
| `upper_limit: float`     | 涨停价        | -           |
| `lower_limit: float`     | 跌停价        | -           |
| `deal_number: int`       | 股票成交笔数  | -           |
| `iopv: float`            | 基金估值      | -           |
| `ask_volumeX`            | 叫卖量        | `X: 1~10`   |
| `ask_priceX`             | 叫卖价        | `X: 1~10`   |
| `bid_priceX`             | 叫买价        | `X: 1~10`   |
| `bid_volumeX`            | 叫买量        | `X: 1~10`   |

## BigTrader 的数据字典

- 买卖方向：`Direction.BUY='1'`，`SELL='2'`，`FinancingBuy='3'`，`SellRepay='4'`，`LoanSell='5'`，`BuyRedeliver='6'`
- 开平标志：`OffsetFlag.OPEN='0'`，`CLOSE='1'`，`CLOSE_TODAY='2'`，`CLOSE_YESTERDAY='3'`
- 委托类型：`OrderType.LIMIT='0'`，`MARKET='U'`

### 委托状态 `OrderStatus`

| 委托状态                | 状态值 | 状态说明 |
| ----------------------- | ------ | -------- |
| `OrderStatus.NOTTRADED` | 0      | 未成交   |
| `PARTTRADED`            | 1      | 部分成交 |
| `ALLTRADED`             | 2      | 全部成交 |
| `PARTCANCELLED`         | 3      | 部分撤单 |
| `CANCELLED`             | 4      | 全部撤单 |
| `REJECTED`              | 5      | 废单     |
| `UNKNOWN`               | 6      | 未知     |
| `NOTPLACE`              | 10     | 未报     |
| `PLACING`               | 11     | 正报     |
| `PENDINGPLACE`          | 12     | 待报     |
| `PARTPENDINGPLACE`      | 15     | 部分待撤 |
| `PENDINGPLACE`          | 16     | 待撤销   |

## 撮合与策略运行结果说明

### 订单撮合处理

本章节介绍基于历史行情的模拟撮合：Bar 行情、快照行情、逐笔行情。支持上交所/深交所上市的 A 股股票、基金、债券、期权、期货等；不支持新股申购、市值配售、增发申购、配股等交易。股票交易费用默认买入 `0.03%`、卖出 `0.13%`（含默认 `0.1%` 印花税），期货按品种费率计算，费率也可在回测模拟中设置。

### 通用规则

- 对于未成交或部分成交委托，可撤单。
- 当天未成交委托收市后自动作废，不参与下一交易日撮合。
- 市价委托未成交部分自动撤销（即成剩撤）。

### 基于 Bar 行情数据撮合规则

- 成交参考价可指定为 `open/close`，分钟回测一般为下单后第一分钟开盘价，或后续分钟撮合时按订单限价。
- 成交数量最大为当次 Bar 成交量；日线通常还会乘一个成交率比例。
- 委托量过大时，模拟撮合结果可能与真实情况严重失真。

### 基于快照行情数据的撮合规则

主要原则：基于快照中的最新价撮合，而不是按买卖盘口价格撮合；发单后使用下一笔快照行情撮合。

#### 买入

- 若最新成交价等于委托价：按委托价成交。
- 若最新成交价低于委托价：按最新价成交。
- 若成交价在买一价或涨停时：不能即时成交，委托进入撮合等待队列，并记录当时买一量；若阶段成交量大于买一量，可成交量为“阶段成交量与当时买一量之差”（用于模拟排队，未考虑买一撤单量）。
- 若涨停板打开且价格低于委托价：按现价成交。

#### 卖出

- 若最新成交价等于委托价：按委托价成交。
- 若最新价高于委托价：按最新价成交。若成交价在卖一价或跌停时，不能即时成交，委托进入等待队列并记录当时卖一量；若阶段成交量大于卖一量，可成交量为两者之差（用于模拟排队，未考虑卖一撤单量）。
- 若跌停板打开且价格高于委托价：按现价成交。
- 当次成交量按两个快照之间的真实成交量计算；若成交量为 0 则不成交。若真实成交量小于委托未成交量，则部分成交，其余继续在队列等待新的成交明细。
- 因此，集合竞价期间报单会在 `09:25:00` 撮合一次，或从 `09:30:00` 开始连续竞价撮合。

示例：`600804(鹏博士)` 上午开市后涨停，用户在 10:10 以涨停价委托买入 100 手，此时成交量 51000 手，涨停板买一量 5000 手。若涨停板未打开，只有阶段成交量大于 5000 手时才可能成交；若成交量到 56010 手，则用户成交 10 手（`56010 - 51000 - 5000`），其余继续等待。

### 基于逐笔成交数据的撮合规则

主要原则：基于逐笔行情中的最新成交价撮合。发单后用下一笔逐笔行情撮合，需配合当前快照盘口信息（收到委托时获取当前盘口）。

#### 买入委托

市价委托：

- 当前在涨停板上：不成交，自动撤销。
- 可成交时：即成剩余，成交量依次从卖一到卖五档。
- 不支持 FOK。

限价委托：

- 委托价 `> 最新价`：成交，成交价 = 最新价；成交量依次从卖一到卖十档，可能生成多笔成交信息。`FIXME`：超出卖十档处理、触发涨停板处理。
- 委托价 `== 最新价`：
  - 成交类型为主动卖：判断当前记录排队数量，排队数量 `<= 0` 则成交，成交量为当次逐笔成交量。`FIXME`：更逼真模式应为“逐笔成交量 - 排队数量”。
  - 成交类型为主动买：不成交。
- 撤单成交（仅深交所）：
  - 撤单价格 `== 买一价`：减少订单队列中买入委托排队数量。
  - 其它撤单：不处理。
- 其它情况：不成交。

#### 卖出委托

市价委托：

- 当前在跌停板上：不成交，自动撤销。
- 可成交时：即成剩余，成交量依次从买一到买五档。
- 不支持 FOK。

限价委托：

- 委托价 `< 最新价`：成交，成交价 = 最新价；成交量依次从买一到买十档，可能生成多笔成交信息。`FIXME`：超出买十档处理、触发跌停板处理。
- 委托价 `== 最新价`：
  - 成交类型为主动买：判断当前记录排队数量，排队数量 `<= 0` 则成交，成交量为当次逐笔成交量。`FIXME`：更逼真模式应为“逐笔成交量 - 排队数量”。
  - 成交类型为主动卖：不成交。
- 撤单成交（仅深交所）：
  - 撤单价格 `== 卖一价`：减少订单队列中的买入委托排队数量。
  - 其它撤单：不处理。
- 其它情况：不成交。

## 回测结果分析

完成策略回测后，会得到回测结果图。图中红色矩形标记部分包含策略主要信息：收益概况、交易详情、每日持仓及收益、输出日志。

### 收益概况

收益概况以折线图展示策略时间序列收益率：黄色曲线为策略收益率，蓝色曲线为沪深 300 基准收益率，底部绿色曲线为持仓占比（仓位）。相对收益率曲线默认不直接显示，可点击图例“相对收益率”绘制。

### 常见指标解释

- 收益率：回测时间段总收益率。例如收益率 30%，本金从 1 万到 1.3 万，盈利 3000 元。
- 年化收益率：策略每年的收益率。例如 2 年总收益 30%，年化约 15%（不考虑复利）。
- 基准收益率：默认以沪深 300 为基准。若策略收益低于基准，说明表现弱于大盘。
- 阿尔法：衡量策略的关键指标之一，越大越好。
- 贝塔：衡量策略的关键指标之一，越小越好。
- 夏普比率：重要指标，兼顾收益与风险，可理解为风险调整后的收益能力。
- 胜率：盈利次数占比，越大越好。
- 盈亏比：平均盈利与平均亏损之比，越大越好。
- 收益波动率：收益率标准差，风险指标之一。
- 最大回撤：净值从历史高点回撤的最大跌幅，衡量极端亏损风险能力。
- 信息比率：常用策略评价指标。

关于回测结果和指标更详细的分析可参照：`策略回测结果指标详解`。
