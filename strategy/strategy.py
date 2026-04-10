import json
from pathlib import Path

from bigmodule import M, I
import dai
import pandas as pd
from tqdm import tqdm

'''
## 策略配置
- **股票池** (`cn_stock_prefactors_community`)
  - 按 `total_market_cap` 升序取前500只
  - 排除 ST、停牌、北交所
  - 股票池作为指标benchmark (过滤和排序都作为策略超额)
  - 每日重算
- **过滤因子**
  - 股票池排除过滤因子=1的标的
  - 每日重算
- **排序因子**
  - 过滤后的股票池, 根据排序因子加权计算得分, 得到最终当日持仓标的
  - TODO: 暂时没有排序因子, 持有所有股票池内标的
- **持仓**
  - 等权分配，每日调仓
- **交易限制**
  - 涨/跌停时不会买入/卖出

过滤因子:
**预期连续两年亏损**:
    数据源: strategy/filter/forecast_2_year_loss/indicator.json
    `forecast_2_year_loss := 前年亏损 AND 去年预亏 AND 年报未发 AND 年报截至前(4月底)`
    - 前年亏损 = `last_parent_net < 0`
    - 去年预亏 = `type ∈ {'首亏', '续亏'}` (年报: `end_date[4:6]=='12'`)
    - 年报未发 = `date < disclosure.actual_date`
    - 年报截至 = `disclosure.actual_date ?? (end_date.year+1, 4, monthend) 次年4月月末`

排序因子: 无

代码编写原则:
1. 回测vector mode应该和实盘incremental mode实现一致, 尽量共享代码和逻辑
2. 每个因子的实现应该尽量独立定义在代码最前, 不要和后面的框架耦合
'''
BACKTEST_START_DATE = "2021-01-01"
BACKTEST_END_DATE = "2026-04-07"


def factor_forecast_2_year_loss(start_date, end_date):
    indicator_path = Path(__file__).resolve().parent / "filter" / "forecast_2_year_loss" / "indicator.json"
    raw_rows = json.loads(indicator_path.read_text())
    assert isinstance(raw_rows, list), f"invalid forecast json: {indicator_path}"

    interval_rows = []
    for item in raw_rows:
        assert isinstance(item, dict) and len(item) == 1, f"invalid forecast item: {item}"
        instrument, intervals = next(iter(item.items()))
        assert isinstance(instrument, str) and instrument, f"invalid instrument: {item}"
        assert isinstance(intervals, list), f"invalid intervals: {item}"
        for interval in intervals:
            assert isinstance(interval, list) and len(interval) == 2, f"invalid interval: {interval}"
            start_date_int, end_date_int = interval
            assert isinstance(start_date_int, int) and isinstance(end_date_int, int), f"invalid interval date: {interval}"
            assert start_date_int <= end_date_int, f"start_date > end_date: {interval}"
            interval_rows.append(
                {
                    "instrument": instrument,
                    "start_date": start_date_int,
                    "end_date": end_date_int,
                }
            )

    start_int = int(start_date.replace("-", ""))
    end_int = int(end_date.replace("-", ""))
    state_df = pd.DataFrame(interval_rows, columns=["instrument", "start_date", "end_date"])
    assert not state_df.empty, "forecast_2_year_loss source is empty"
    source_start_int = int(state_df["start_date"].min())
    source_end_int = int(state_df["end_date"].max())
    assert source_start_int <= start_int, f"tushare coverage start not enough: {source_start_int} > {start_int}"
    assert source_end_int >= end_int, f"tushare coverage end not enough: {source_end_int} < {end_int}"
    state_df = state_df[(state_df["end_date"] >= start_int) & (state_df["start_date"] <= end_int)].copy()
    assert not state_df.empty, "forecast_2_year_loss has no interval in backtest range"
    state_df["value"] = 1
    return {
        "name": "forecast_2_year_loss",
        "kind": "interval",
        "data": state_df[["instrument", "start_date", "end_date", "value"]],
    }


def prepare_filter_states(start_date, end_date):
    factor_builders = [
        factor_forecast_2_year_loss,
    ]
    states = []
    state_names = set()
    for factor_builder in factor_builders:
        state = factor_builder(start_date, end_date)
        assert isinstance(state, dict), f"invalid state type: {factor_builder.__name__}"
        assert {"name", "kind", "data"}.issubset(state.keys()), f"invalid state keys: {factor_builder.__name__}"
        assert isinstance(state["name"], str) and state["name"], f"invalid state name: {factor_builder.__name__}"
        assert state["name"] not in state_names, f"duplicated factor name: {state['name']}"
        assert state["kind"] in {"interval", "daily"}, f"invalid state kind: {state['kind']}"
        assert isinstance(state["data"], pd.DataFrame), f"invalid state data: {state['name']}"
        states.append(state)
        state_names.add(state["name"])
    return states


def calc_filter_on_day(trade_date, universe_today_df, states):
    factor_df = universe_today_df[["instrument"]].drop_duplicates().copy()
    trade_date_int = int(trade_date.replace("-", ""))

    for state in states:
        factor_name = state["name"]
        state_kind = state["kind"]
        state_df = state["data"]

        if state_df.empty:
            factor_df[factor_name] = 0
            continue

        if state_kind == "interval":
            assert {"instrument", "start_date", "end_date", "value"}.issubset(state_df.columns), f"invalid interval state: {factor_name}"
            day_factor_df = factor_df[["instrument"]].merge(state_df, on="instrument", how="left")
            day_factor_df[factor_name] = (
                ((trade_date_int >= day_factor_df["start_date"]) & (trade_date_int <= day_factor_df["end_date"])).astype(int)
                * day_factor_df["value"].fillna(0).astype(int)
            )
            day_factor_df = day_factor_df.groupby("instrument", as_index=False)[factor_name].max()
        else:
            assert {"date", "instrument", "value"}.issubset(state_df.columns), f"invalid daily state: {factor_name}"
            day_factor_df = state_df[state_df["date"] == trade_date][["instrument", "value"]].copy()
            day_factor_df = day_factor_df.groupby("instrument", as_index=False)["value"].max()
            day_factor_df = day_factor_df.rename(columns={"value": factor_name})

        factor_df = factor_df.merge(day_factor_df, on="instrument", how="left")
        factor_df[factor_name] = factor_df[factor_name].fillna(0).astype(int)

    return factor_df


def build_target_on_day(trade_date, universe_today_df, states):
    factor_df = calc_filter_on_day(trade_date, universe_today_df, states)
    merged_df = universe_today_df.merge(factor_df, on="instrument", how="left")
    factor_cols = [state["name"] for state in states]
    target_df = merged_df[(merged_df[factor_cols] == 0).all(axis=1)].copy()

    if target_df.empty:
        target_df["position"] = pd.Series(dtype=float)
        return target_df[["instrument", "position"]]

    target_df["position"] = 1.0 / len(target_df)
    return target_df[["instrument", "position"]]


def bt_init(context):
    from bigtrader.finance.commission import PerOrder
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    context.factor_states = prepare_filter_states(start_date=BACKTEST_START_DATE, end_date=BACKTEST_END_DATE)
    context.universe_by_date = {
        date: day_df[["instrument", "total_market_cap"]].reset_index(drop=True)
        for date, day_df in context.data.groupby("date", sort=False)
    }
    assert len(context.universe_by_date) > 0, "bigquant universe is empty in backtest range"
    universe_dates = sorted(context.universe_by_date.keys())
    assert universe_dates[0] <= BACKTEST_END_DATE, "bigquant coverage starts after backtest end"
    assert universe_dates[-1] >= BACKTEST_START_DATE, "bigquant coverage ends before backtest start"

    price_limit_sql = """
    SELECT date, instrument, price_limit_status
    FROM cn_stock_status
    WHERE price_limit_status IN (1, 3)
    """
    price_limit_df = dai.query(price_limit_sql, filters={"date": [BACKTEST_START_DATE, BACKTEST_END_DATE]}).df()
    price_limit_df["date"] = price_limit_df["date"].astype(str).str[:10]
    context.price_limit_set = set(zip(price_limit_df["date"], price_limit_df["instrument"]))

    context.progress_total_days = len(context.universe_by_date)
    context.progress_done_days = 0
    context.progress_bar = tqdm(total=context.progress_total_days, desc="backtest", unit="day")


def bt_pre(context, data):
    pass


def bt_tick(context, tick):
    pass


def bt_bar(context, data):
    trade_date = data.current_dt.strftime("%Y-%m-%d")
    context.progress_done_days += 1
    assert context.progress_done_days <= context.progress_total_days
    context.progress_bar.update(1)
    if context.progress_done_days == context.progress_total_days:
        context.progress_bar.close()

    if not context.rebalance_period.is_signal_date(data.current_dt.date()):
        return

    universe_today_df = context.universe_by_date.get(trade_date)
    if universe_today_df is None:
        universe_today_df = context.data[context.data["date"] == trade_date][["instrument", "total_market_cap"]].copy()
    target_df = build_target_on_day(trade_date, universe_today_df, context.factor_states)
    target_instruments = set(target_df["instrument"])
    holding_instruments = set(context.get_account_positions().keys())

    def is_price_limited(inst):
        return (trade_date, inst) in context.price_limit_set

    for instrument in sorted(holding_instruments - target_instruments):
        if is_price_limited(instrument):
            continue
        context.order_target_percent(instrument, 0)

    for _, row in target_df.iterrows():
        if is_price_limited(row.instrument):
            continue
        position = 0.0 if pd.isnull(row.position) else float(row.position)
        context.order_target_percent(row.instrument, position)


def bt_trade(context, trade):
    pass


def bt_order(context, order):
    pass


def bt_post(context, data):
    pass

# ==================== vector / incremental 共享信号 ====================
universe_sql = """
SELECT
    date,
    instrument,
    total_market_cap
FROM cn_stock_prefactors_community
WHERE st_status = 0
  AND suspended = 0
  AND is_bz50 = 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_market_cap ASC) <= 500
ORDER BY date, instrument
"""
universe_df = dai.query(universe_sql, filters={"date": [BACKTEST_START_DATE, BACKTEST_END_DATE]}).df()
assert {"date", "instrument", "total_market_cap"}.issubset(universe_df.columns)
universe_df["date"] = universe_df["date"].astype(str).str[:10]
print(f"股票池记录数：{len(universe_df)}")
stock_data_ds = dai.DataSource.write_bdb(universe_df)

# ========== 回测 ==========
m5 = M.bigtrader.v30(
    data=stock_data_ds,
    start_date=BACKTEST_START_DATE,
    end_date=BACKTEST_END_DATE,
    initialize=bt_init,
    before_trading_start=bt_pre,
    handle_tick=bt_tick,
    handle_data=bt_bar,
    handle_trade=bt_trade,
    handle_order=bt_order,
    after_trading=bt_post,
    capital_base=1000000,
    frequency="daily",
    product_type="股票",
    rebalance_period_type="交易日",
    rebalance_period_days="1",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="标准模式",
    before_start_days=0,
    volume_limit=1,
    order_price_field_buy="open",
    order_price_field_sell="open",
    benchmark="沪深300指数",
    plot_charts=True,
    debug=False,
    backtest_only=False,
    m_name="m5"
)