import json
from pathlib import Path

from bigmodule import M, I
import dai
import pandas as pd
from tqdm import tqdm

STRATEGY_DIR = Path.cwd()

BACKTEST_START_DATE = "2017-01-01"
BACKTEST_END_DATE = "2026-04-07"
UNIVERSE_SIZE = 500
HOLD_N = 50
EXIT_RATIO = 1
CAPITAL_BASE = 1000000

'''
## 策略配置
- **股票池** (`cn_stock_basic_selector` + `cn_stock_prefactors_community`)
  - 基础过滤: 上交所/深交所, 主板/创业板/科创板, 排除ST, 排除停牌
  - 按 `total_market_cap` 升序取前 `UNIVERSE_SIZE` 只
  - 字段: `close`, `upper_limit`, `lower_limit` (涨跌停判断)
  - 股票池作为指标benchmark (过滤和排序都作为策略超额)
  - 每日重算
- **过滤因子**
  - 股票池排除过滤因子=1的标的
  - 每日重算
- **排序因子**
  - 过滤后的股票池, 根据排序因子加权计算得分, 得到最终当日持仓标的
  - 每日重算
- **持仓/交易**
  - 预期持仓标的数: `HOLD_N`
  - 预期仓位: 保持100%
  - 先卖出: 判断已持仓标的是否离开前 `HOLD_N * EXIT_RATIO` 名, 如果离开则卖出(同时服从交易限制)
  - 再买入: 除开仍旧持仓的M只标的, 找出因子排名前 `HOLD_N - M` 只标的(同时服从交易限制), 将剩余资金均分至新买入标的(已持仓标的不调仓)
- **交易限制**
  - 涨停时不会买入(做不到)
  - 跌停时不会卖出(做不到)
  - 涨停时不会卖出(预期第二天有超额收益)
  - 跌停时不会买入(预期第二天有超额风险)

过滤因子:
**预期连续两年亏损**:
    数据源: strategy/filter/forecast_2_year_loss/indicator.json
    `forecast_2_year_loss := 前年亏损 AND 去年预亏 AND 年报未发 AND 年报截至前(4月底)`
    - 前年亏损 = `last_parent_net < 0`
    - 去年预亏 = `type ∈ {'首亏', '续亏'}` (年报: `end_date[4:6]=='12'`)
    - 年报未发 = `date < disclosure.actual_date`
    - 年报截至 = `disclosure.actual_date ?? (end_date.year+1, 4, monthend) 次年4月月末`

排序因子:
**总市值**: (优先:小; 权重:100%)
    数据源: cn_stock_prefactors_community.total_market_cap

代码编写原则:
1. 回测vector mode应该和实盘incremental mode实现一致, 尽量共享代码和逻辑
2. 每个因子的实现应该尽量独立定义在代码最前, 不要和后面的框架耦合
'''


def factor_forecast_2_year_loss(start_date, end_date):
    indicator_path = STRATEGY_DIR / "filter" / \
        "forecast_2_year_loss" / "indicator.json"
    raw_rows = json.loads(indicator_path.read_text())
    assert isinstance(
        raw_rows, list), f"invalid forecast json: {indicator_path}"

    interval_rows = []
    for item in raw_rows:
        assert isinstance(item, dict) and len(
            item) == 1, f"invalid forecast item: {item}"
        instrument, intervals = next(iter(item.items()))
        assert isinstance(
            instrument, str) and instrument, f"invalid instrument: {item}"
        assert isinstance(intervals, list), f"invalid intervals: {item}"
        for interval in intervals:
            assert isinstance(interval, list) and len(
                interval) == 2, f"invalid interval: {interval}"
            start_date_int, end_date_int = interval
            assert isinstance(start_date_int, int) and isinstance(
                end_date_int, int), f"invalid interval date: {interval}"
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
    state_df = pd.DataFrame(interval_rows, columns=[
                            "instrument", "start_date", "end_date"])
    assert not state_df.empty, "forecast_2_year_loss source is empty"
    source_start_int = int(state_df["start_date"].min())
    source_end_int = int(state_df["end_date"].max())
    assert source_start_int <= start_int, f"tushare coverage start not enough: {source_start_int} > {start_int}"
    assert source_end_int >= end_int, f"tushare coverage end not enough: {source_end_int} < {end_int}"
    state_df = state_df[(state_df["end_date"] >= start_int) & (
        state_df["start_date"] <= end_int)].copy()
    assert not state_df.empty, "forecast_2_year_loss has no interval in backtest range"
    state_df["value"] = 1
    return {
        "name": "forecast_2_year_loss",
        "kind": "interval",
        "data": state_df[["instrument", "start_date", "end_date", "value"]],
    }


def prepare_filter_states(start_date, end_date, trading_dates):
    """
    trading_dates: 回测期间的交易日列表 (YYYY-MM-DD 格式)，用于展开 interval
    """
    factor_builders = [
        factor_forecast_2_year_loss,
    ]
    states = []
    state_names = set()
    trading_date_ints = sorted(int(d.replace("-", "")) for d in trading_dates)

    for factor_builder in factor_builders:
        state = factor_builder(start_date, end_date)
        assert isinstance(
            state, dict), f"invalid state type: {factor_builder.__name__}"
        assert {"name", "kind", "data"}.issubset(
            state.keys()), f"invalid state keys: {factor_builder.__name__}"
        assert isinstance(
            state["name"], str) and state["name"], f"invalid state name: {factor_builder.__name__}"
        assert state["name"] not in state_names, f"duplicated factor name: {state['name']}"
        assert state["kind"] in {
            "interval", "daily"}, f"invalid state kind: {state['kind']}"
        assert isinstance(
            state["data"], pd.DataFrame), f"invalid state data: {state['name']}"

        if state["kind"] == "interval":
            filter_set = set()
            for _, row in state["data"].iterrows():
                inst = row["instrument"]
                start_int, end_int = row["start_date"], row["end_date"]
                for d_int in trading_date_ints:
                    if start_int <= d_int <= end_int:
                        filter_set.add((d_int, inst))
            state["filter_set"] = filter_set

        states.append(state)
        state_names.add(state["name"])
    return states


def calc_filter_on_day(trade_date, universe_today_df, states):
    instruments = universe_today_df["instrument"].drop_duplicates().tolist()
    trade_date_int = int(trade_date.replace("-", ""))

    factor_values = {inst: {} for inst in instruments}

    for state in states:
        factor_name = state["name"]
        state_kind = state["kind"]

        if state_kind == "interval":
            filter_set = state["filter_set"]
            for inst in instruments:
                factor_values[inst][factor_name] = 1 if (
                    trade_date_int, inst) in filter_set else 0
        else:
            state_df = state["data"]
            day_values = {}
            if not state_df.empty:
                day_df = state_df[state_df["date"] == trade_date]
                for inst, val in zip(day_df["instrument"], day_df["value"]):
                    day_values[inst] = max(day_values.get(inst, 0), val)
            for inst in instruments:
                factor_values[inst][factor_name] = day_values.get(inst, 0)

    factor_df = pd.DataFrame([
        {"instrument": inst, **vals} for inst, vals in factor_values.items()
    ])
    return factor_df


def build_target_on_day(trade_date, universe_today_df, states):
    """
    返回:
        top_n_instruments: 前N只标的（买入目标）
        top_exit_instruments: 前N*EXIT_RATIO只标的（退出阈值）
    """
    factor_df = calc_filter_on_day(trade_date, universe_today_df, states)
    merged_df = universe_today_df.merge(factor_df, on="instrument", how="left")
    factor_cols = [state["name"] for state in states]
    filtered_df = merged_df[(merged_df[factor_cols] == 0).all(axis=1)].copy()

    if filtered_df.empty:
        return set(), set()

    sorted_df = filtered_df.sort_values("total_market_cap", ascending=True)
    top_n_instruments = set(sorted_df.head(HOLD_N)["instrument"])
    exit_threshold = int(HOLD_N * EXIT_RATIO)
    top_exit_instruments = set(sorted_df.head(exit_threshold)["instrument"])
    return top_n_instruments, top_exit_instruments


def bt_init(context):
    from bigtrader.finance.commission import PerOrder
    context.set_commission(
        PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    context.data["date"] = context.data["date"].dt.strftime("%Y-%m-%d")
    context.universe_by_date = {
        date: day_df[["instrument", "total_market_cap"]].reset_index(drop=True)
        for date, day_df in context.data.groupby("date", sort=False)
    }
    context.price_limit_by_date = {
        date: {
            row["instrument"]: {
                "close": row["close"],
                "upper_limit": row["upper_limit"],
                "lower_limit": row["lower_limit"],
            }
            for _, row in day_df.iterrows()
        }
        for date, day_df in context.data.groupby("date", sort=False)
    }
    trading_dates = list(context.universe_by_date.keys())
    context.factor_states = prepare_filter_states(
        start_date=BACKTEST_START_DATE, end_date=BACKTEST_END_DATE, trading_dates=trading_dates
    )
    assert len(
        context.universe_by_date) > 0, "bigquant universe is empty in backtest range"
    universe_dates = sorted(context.universe_by_date.keys())
    assert universe_dates[0] <= BACKTEST_END_DATE, "bigquant coverage starts after backtest end"
    assert universe_dates[-1] >= BACKTEST_START_DATE, "bigquant coverage ends before backtest start"

    context.progress_total_days = len(context.universe_by_date)
    context.progress_done_days = 0
    context.progress_bar = tqdm(
        total=context.progress_total_days, desc="backtest", unit="day")


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
        universe_today_df = context.data[context.data["date"] == trade_date][[
            "instrument", "total_market_cap"]].copy()
    price_limit_today = context.price_limit_by_date.get(trade_date, {})

    def is_limit_up(inst):
        info = price_limit_today.get(inst)
        if info is None:
            return False
        return info["close"] >= info["upper_limit"]

    def is_limit_down(inst):
        info = price_limit_today.get(inst)
        if info is None:
            return False
        return info["close"] <= info["lower_limit"]

    top_n_instruments, top_exit_instruments = build_target_on_day(
        trade_date, universe_today_df, context.factor_states)
    holding_instruments = set(context.get_account_positions().keys())

    # 先卖出: 离开退出阈值的持仓（跌停不卖，涨停也不卖）
    to_sell = []
    for inst in holding_instruments:
        if inst not in top_exit_instruments:
            if is_limit_down(inst):
                continue  # 跌停不卖
            if is_limit_up(inst):
                continue  # 涨停不卖（预期次日有超额收益）
            to_sell.append(inst)

    for inst in sorted(to_sell):
        context.order_target_percent(inst, 0)

    # 再买入: 计算目标持仓
    remaining_holding = holding_instruments - set(to_sell)
    slots_available = HOLD_N - len(remaining_holding)

    if slots_available > 0:
        # 从top_n中找出不在持仓中的标的
        candidates = top_n_instruments - remaining_holding
        # 排除涨停和跌停的
        candidates = {inst for inst in candidates if not is_limit_up(
            inst) and not is_limit_down(inst)}
        to_buy = sorted(candidates)[:slots_available]
    else:
        to_buy = []

    # 只对新买入标的分配剩余资金，已持仓标的不动
    if to_buy:
        positions = context.get_account_positions()
        used_value = sum(
            positions[inst].market_value
            for inst in remaining_holding
            if inst in positions
        )
        total_value = context.portfolio.portfolio_value
        available_value = total_value - used_value
        position_per_new = available_value / len(to_buy) / total_value
        for inst in to_buy:
            context.order_target_percent(inst, position_per_new)


def bt_trade(context, trade):
    pass


def bt_order(context, order):
    pass


def bt_post(context, data):
    pass


# ==================== 模块链 ====================
# 1. 基础股票池过滤（交易所、板块、ST、停牌）
SW2021_ALL_INDUSTRIES = [
    "基础化工", "有色金属", "建筑材料", "建筑装饰",
    "机械设备", "电子", "汽车", "家用电器", "食品饮料", "纺织服饰",
    "轻工制造", "医药生物", "公用事业", "商贸零售",
    "社会服务", "非银金融", "综合", "电力设备", "国防军工",
    "计算机", "传媒", "通信", "煤炭", "石油石化", "美容护理",
    "农林牧渔", "钢铁", "银行",
    # 过滤弹性差的行业(在市值底部存在过久)
    # "环保", "交通运输", "房地产",
]
m1 = M.cn_stock_basic_selector.v7(
    exchanges=["上交所", "深交所"],
    list_sectors=["主板", "创业板", "科创板"],
    indexes=[],
    st_statuses=["正常"],
    margin_tradings=["两融标的", "非两融标的"],
    sw2021_industries=SW2021_ALL_INDUSTRIES,
    drop_suspended=True,
    m_name="m1"
)

# 2. 用基础股票池SQL过滤，加市值排序取前 UNIVERSE_SIZE + 涨跌停字段
basic_pool_sql = m1.data.read()["sql"]
basic_pool_sql = basic_pool_sql.replace("AND ()", "")  # 修复 indexes=[] 导致的空条件

universe_sql = f"""
WITH basic_pool AS (
    {basic_pool_sql}
)
SELECT
    date,
    instrument,
    total_market_cap,
    close,
    upper_limit,
    lower_limit
FROM cn_stock_prefactors_community
WHERE (date, instrument) IN (SELECT date, instrument FROM basic_pool)
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_market_cap ASC) <= {UNIVERSE_SIZE}
ORDER BY date, instrument
"""

universe_df = dai.query(universe_sql, filters={
                        "date": [BACKTEST_START_DATE, BACKTEST_END_DATE]}).df()
assert {"date", "instrument", "total_market_cap", "close",
        "upper_limit", "lower_limit"}.issubset(universe_df.columns)
universe_df["date"] = pd.to_datetime(universe_df["date"]).dt.normalize()
print(f"股票池记录数：{len(universe_df)}")
stock_data_ds = dai.DataSource.write_bdb(universe_df)

# ========== 回测 ==========
m5 = M.bigtrader.v30(
    data=stock_data_ds,
    start_date=None,
    end_date=None,
    initialize=bt_init,
    before_trading_start=bt_pre,
    handle_tick=bt_tick,
    handle_data=bt_bar,
    handle_trade=bt_trade,
    handle_order=bt_order,
    after_trading=bt_post,
    capital_base=CAPITAL_BASE,
    frequency="daily",
    product_type="股票",
    rebalance_period_type="交易日",
    rebalance_period_days="1",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="标准模式",
    before_start_days=0,
    volume_limit=1,
    order_price_field_buy="close",
    order_price_field_sell="close",
    benchmark="中证1000指数",
    plot_charts=True,
    debug=False,
    backtest_only=False,
    m_name="m5"
)
