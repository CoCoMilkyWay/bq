import json
from pathlib import Path

from bigmodule import M, I
import dai
import pandas as pd
from tqdm import tqdm

STRATEGY_DIR = Path.cwd()

BACKTEST_START_DATE = "2017-01-01"
BACKTEST_END_DATE = "2026-04-07"
UNIVERSE_SIZE = 100
HOLD_N = 40
EXIT_RATIO = 1.2
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
  - 先卖出: 离开前 `HOLD_N * EXIT_RATIO` 名(同时服从交易限制)
  - 再买入: 除开仍旧持仓的M只标的, 找出因子排名前 `HOLD_N - M` 只标的(同时服从交易限制), 将剩余资金均分至新买入标的(已持仓标的不调仓)
- **交易限制**
  - 涨停时不会买入(做不到)
  - 跌停时不会卖出(做不到)
  - 涨停时不会卖出(预期第二天有超额收益)
  - 跌停时不会买入(预期第二天有超额风险)

过滤因子: (统一从 filter/{name}/indicator.json 加载)
**预期连续两年亏损** (forecast_2_year_loss):
    数据源: tushare forecast + tushare disclosure
    `forecast_2_year_loss := 前年亏损 AND 去年预亏 AND 年报未发 AND 年报截至前(4月底)`
    - 前年亏损 = `last_parent_net < 0`
    - 去年预亏 = `type ∈ {'首亏', '续亏'}` (年报: `end_date[4:6]=='12'`)
    - 年报未发 = `date < disclosure.actual_date`
    - 年报截至 = `disclosure.actual_date ?? (end_date.year+1, 4, monthend) 次年4月月末`
**预期ST** (forecast_st):
    数据源: tushare forecast + bigquant cn_stock_financial_ttm_shift + bigquant cn_stock_basic_info
    `forecast_st := 预亏 AND TTM营收<阈值 AND 年报未发 AND 年报截至前(4月底) AND 21年后`
    - 预亏 = `type ∈ {'首亏', '续亏'}` (年报: `end_date[4:6]=='12'`)
    - TTM营收<阈值: 使用公告日可用的最新TTM营收, 阈值一般1亿, 24年起主板3亿
    - 板块判断: cn_stock_basic_info.list_sector (主板=1)
    - 年报未发/截至: 同上
    - 21年后适用: report_year >= 2021 AND ann_date >= 20210101
**风险警示** (risk_warning):
    数据源: bigquant cn_stock_status
    `risk_warning := is_risk_warning = 1`
    - 风险警示公告发布后标记

排序因子:
**总市值**: (优先:小; 权重:100%)
    数据源: cn_stock_prefactors_community.total_market_cap

代码编写原则:
1. 回测vector mode应该和实盘incremental mode实现一致, 尽量共享代码和逻辑
2. 每个因子的实现应该尽量独立定义在代码最前, 不要和后面的框架耦合
'''


def load_interval_filter(name: str, start_date: str, end_date: str):
    """
    通用的interval类型过滤因子加载函数
    从 filter/{name}/indicator.json 加载数据
    """
    indicator_path = STRATEGY_DIR / "filter" / name / "indicator.json"
    if not indicator_path.exists():
        return None
    raw_rows = json.loads(indicator_path.read_text())
    assert isinstance(raw_rows, list), f"invalid json: {indicator_path}"

    interval_rows = []
    for item in raw_rows:
        assert isinstance(item, dict) and len(item) == 1, f"invalid item: {item}"
        instrument, intervals = next(iter(item.items()))
        assert isinstance(instrument, str) and instrument, f"invalid instrument: {item}"
        assert isinstance(intervals, list), f"invalid intervals: {item}"
        for interval in intervals:
            assert isinstance(interval, list) and len(interval) == 2, f"invalid interval: {interval}"
            start_date_int, end_date_int = interval
            assert isinstance(start_date_int, int) and isinstance(end_date_int, int), f"invalid interval date: {interval}"
            assert start_date_int <= end_date_int, f"start_date > end_date: {interval}"
            interval_rows.append({
                "instrument": instrument,
                "start_date": start_date_int,
                "end_date": end_date_int,
            })

    if not interval_rows:
        return None

    start_int = int(start_date.replace("-", ""))
    end_int = int(end_date.replace("-", ""))
    state_df = pd.DataFrame(interval_rows, columns=["instrument", "start_date", "end_date"])
    state_df = state_df[(state_df["end_date"] >= start_int) & (state_df["start_date"] <= end_int)].copy()
    if state_df.empty:
        return None
    state_df["value"] = 1
    return {
        "name": name,
        "kind": "interval",
        "data": state_df[["instrument", "start_date", "end_date", "value"]],
    }


FILTER_NAMES = [
    "forecast_2_year_loss",
    "forecast_st",
    "risk_warning",
]


def prepare_filter_states(start_date, end_date, trading_dates):
    """
    trading_dates: 回测期间的交易日列表 (YYYY-MM-DD 格式)，用于展开 interval
    """
    states = []
    state_names = set()
    trading_date_ints = sorted(int(d.replace("-", "")) for d in trading_dates)

    for filter_name in FILTER_NAMES:
        state = load_interval_filter(filter_name, start_date, end_date)
        if state is None:
            continue
        assert isinstance(state, dict), f"invalid state type: {filter_name}"
        assert {"name", "kind", "data"}.issubset(
            state.keys()), f"invalid state keys: {filter_name}"
        assert isinstance(
            state["name"], str) and state["name"], f"invalid state name: {filter_name}"
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


def build_target_on_day(trade_date_int, instruments, market_caps, filter_sets):
    """
    Per-bar 计算，与实盘共享逻辑
    参数:
        trade_date_int: 交易日 YYYYMMDD 整数
        instruments: np.ndarray 或 list
        market_caps: np.ndarray 或 list
        filter_sets: list of set[(date_int, instrument)]
    返回:
        top_n_instruments: set
        top_exit_instruments: set
        rank_map: {instrument: rank}
    """
    filtered_pairs = [
        (inst, cap)
        for inst, cap in zip(instruments, market_caps)
        if not any((trade_date_int, inst) in fs for fs in filter_sets)
    ]

    if not filtered_pairs:
        return set(), set(), {}

    filtered_pairs.sort(key=lambda x: x[1])
    rank_map = {inst: idx + 1 for idx, (inst, _) in enumerate(filtered_pairs)}
    top_n_instruments = {inst for inst, _ in filtered_pairs[:HOLD_N]}
    exit_threshold = int(HOLD_N * EXIT_RATIO)
    top_exit_instruments = {inst for inst, _ in filtered_pairs[:exit_threshold]}
    return top_n_instruments, top_exit_instruments, rank_map


def bt_init(context):
    from bigtrader.finance.commission import PerOrder
    context.set_commission(
        PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    context.data["date"] = context.data["date"].dt.strftime("%Y-%m-%d")

    # 预处理 universe: (instruments, market_caps) 元组，避免 DataFrame 操作
    context.universe_by_date = {}
    for date, day_df in context.data.groupby("date", sort=False):
        context.universe_by_date[date] = (
            day_df["instrument"].values,
            day_df["total_market_cap"].values,
        )

    # 预处理 price_limit: 向量化构建，避免 iterrows
    context.price_limit_by_date = {}
    for date, day_df in context.data.groupby("date", sort=False):
        insts = day_df["instrument"].values
        closes = day_df["close"].values
        uppers = day_df["upper_limit"].values
        lowers = day_df["lower_limit"].values
        context.price_limit_by_date[date] = {
            inst: (close, upper, lower)
            for inst, close, upper, lower in zip(insts, closes, uppers, lowers)
        }

    trading_dates = list(context.universe_by_date.keys())
    factor_states = prepare_filter_states(
        start_date=BACKTEST_START_DATE, end_date=BACKTEST_END_DATE, trading_dates=trading_dates
    )
    # 提取 filter_sets 列表供 per-bar 使用
    context.filter_sets = [s["filter_set"] for s in factor_states if s["kind"] == "interval"]
    assert len(
        context.universe_by_date) > 0, "bigquant universe is empty in backtest range"
    universe_dates = sorted(context.universe_by_date.keys())
    assert universe_dates[0] <= BACKTEST_END_DATE, "bigquant coverage starts after backtest end"
    assert universe_dates[-1] >= BACKTEST_START_DATE, "bigquant coverage ends before backtest start"

    context.progress_total_days = len(context.universe_by_date)
    context.progress_done_days = 0
    context.progress_bar = tqdm(
        total=context.progress_total_days, desc="backtest", unit="day")

    # 交易诊断初始化
    context.trade_diag = {
        "benchmark_daily_return": _diag_benchmark_daily,
        "trading_dates": _diag_trading_dates,
        "open_records": {},
        "closed_trades": [],
    }


def bt_pre(context, data):
    pass


def bt_tick(context, tick):
    pass


def decide_trades_on_day(
    holding_instruments,
    top_n_instruments,
    top_exit_instruments,
    is_tradable,
):
    """
    Per-day 交易决策，回测与实盘共享
    参数:
        holding_instruments: set, 当前持仓标的
        top_n_instruments: set, 排名前 HOLD_N 的标的
        top_exit_instruments: set, 排名前 HOLD_N * EXIT_RATIO 的标的
        is_tradable: Callable(inst) -> bool, 判断是否可交易（非涨跌停）
    返回:
        to_sell: list, 需要卖出的标的
        to_buy: list, 需要买入的标的
    """
    to_sell = [
        inst for inst in holding_instruments
        if inst not in top_exit_instruments and is_tradable(inst)
    ]
    remaining_holding = holding_instruments - set(to_sell)
    slots_available = HOLD_N - len(remaining_holding)
    if slots_available > 0:
        candidates = top_n_instruments - remaining_holding
        candidates = [inst for inst in candidates if is_tradable(inst)]
        to_buy = sorted(candidates)[:slots_available]
    else:
        to_buy = []
    return sorted(to_sell), to_buy


def bt_bar(context, data):
    trade_date = data.current_dt.strftime("%Y-%m-%d")
    context.progress_done_days += 1
    assert context.progress_done_days <= context.progress_total_days
    context.progress_bar.update(1)
    if context.progress_done_days == context.progress_total_days:
        context.progress_bar.close()

    if not context.rebalance_period.is_signal_date(data.current_dt.date()):
        return

    universe_today = context.universe_by_date.get(trade_date)
    assert universe_today is not None, f"no universe for {trade_date}"
    instruments, market_caps = universe_today
    price_limit_today = context.price_limit_by_date.get(trade_date, {})

    def is_tradable(inst):
        info = price_limit_today.get(inst)
        if info is None:
            return True  # 不在 universe 中（如已持仓标的被调出），允许交易
        close, upper, lower = info
        return close < upper and close > lower

    trade_date_int = int(trade_date.replace("-", ""))
    top_n_instruments, top_exit_instruments, _ = build_target_on_day(
        trade_date_int, instruments, market_caps, context.filter_sets)
    holding_instruments = set(context.get_account_positions().keys())

    to_sell, to_buy = decide_trades_on_day(
        holding_instruments, top_n_instruments, top_exit_instruments, is_tradable)

    for inst in to_sell:
        context.order_target_percent(inst, 0)

    if to_buy:
        positions = context.get_account_positions()
        remaining_holding = holding_instruments - set(to_sell)
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
    inst = trade.instrument
    direction = trade.direction
    diag = context.trade_diag

    if direction == "1":  # BUY
        diag["open_records"][inst] = {
            "open_date": str(trade.trade_date),
            "open_price": trade.filled_price,
        }
    elif direction == "2":  # SELL
        open_rec = diag["open_records"].pop(inst, None)
        if open_rec is None:
            return
        open_date_str = open_rec["open_date"]
        open_date_str = f"{open_date_str[:4]}-{open_date_str[4:6]}-{open_date_str[6:8]}"
        close_date_str = str(trade.trade_date)
        close_date_str = f"{close_date_str[:4]}-{close_date_str[4:6]}-{close_date_str[6:8]}"
        open_price = open_rec["open_price"]
        close_price = trade.filled_price

        trading_dates = diag["trading_dates"]
        benchmark_daily = diag["benchmark_daily_return"]
        try:
            open_idx = trading_dates.index(open_date_str)
            close_idx = trading_dates.index(close_date_str)
        except ValueError:
            return
        holding_days = close_idx - open_idx
        if holding_days <= 0:
            return

        total_return = (close_price - open_price) / open_price
        daily_return = total_return / holding_days

        benchmark_cum = 0.0
        for i in range(open_idx + 1, close_idx + 1):
            d = trading_dates[i]
            benchmark_cum += benchmark_daily.get(d, 0.0)
        daily_benchmark = benchmark_cum / holding_days
        daily_excess = daily_return - daily_benchmark

        diag["closed_trades"].append({
            "instrument": inst,
            "open_date": open_date_str,
            "close_date": close_date_str,
            "holding_days": holding_days,
            "total_return": total_return,
            "daily_return": daily_return,
            "daily_benchmark": daily_benchmark,
            "daily_excess": daily_excess,
        })


def bt_order(context, order):
    pass


def bt_post(context, data):
    if context.progress_done_days != context.progress_total_days:
        return
    diag = context.trade_diag
    closed_trades = diag["closed_trades"]
    if not closed_trades:
        print("\n交易诊断: 无已平仓交易记录")
        return
    sorted_trades = sorted(closed_trades, key=lambda x: x["daily_excess"])
    print(f"\n========== 交易诊断: 超额收益最差的 30 笔交易 ==========")
    print(f"{'标的':<12} {'开仓日期':<12} {'平仓日期':<12} {'持仓天数':>8} {'总收益%':>10} {'日均收益%':>10} {'日均基准%':>10} {'日均超额%':>10}")
    print("-" * 100)
    for t in sorted_trades[:30]:
        print(f"{t['instrument']:<12} {t['open_date']:<12} {t['close_date']:<12} {t['holding_days']:>8} {t['total_return']*100:>10.2f} {t['daily_return']*100:>10.4f} {t['daily_benchmark']*100:>10.4f} {t['daily_excess']*100:>10.4f}")
    print(f"========== 交易诊断结束，共 {len(closed_trades)} 笔已平仓交易 ==========")


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

# ==================== 交易诊断 (独立模块，未来可删除) ====================
# 计算 universe 每日平均收益率作为 benchmark
# 注意: 调出股票的当日收益未计入(需额外数据), 这会导致 benchmark 略微偏低, 超额略微偏高
_diag_df = universe_df.copy()
_diag_df["date_str"] = _diag_df["date"].dt.strftime("%Y-%m-%d")
_diag_trading_dates = sorted(_diag_df["date_str"].unique().tolist())
_diag_date_to_idx = {d: i for i, d in enumerate(_diag_trading_dates)}

_diag_df = _diag_df.sort_values(["instrument", "date_str"])
_diag_df["prev_close"] = _diag_df.groupby("instrument")["close"].shift(1)
_diag_df["prev_date_str"] = _diag_df.groupby("instrument")["date_str"].shift(1)

# 只有连续两个交易日都在 universe 中才计算收益率 (避免调出后再调入导致的跨日计算错误)
_diag_df["cur_idx"] = _diag_df["date_str"].map(_diag_date_to_idx)
_diag_df["prev_idx"] = _diag_df["prev_date_str"].map(_diag_date_to_idx)
_diag_df["is_consecutive"] = (_diag_df["cur_idx"] == _diag_df["prev_idx"] + 1)
_diag_df.loc[~_diag_df["is_consecutive"], "prev_close"] = float("nan")

_diag_df["daily_return"] = (_diag_df["close"] - _diag_df["prev_close"]) / _diag_df["prev_close"]
_diag_benchmark_daily = _diag_df.groupby("date_str")["daily_return"].mean().to_dict()
del _diag_df, _diag_date_to_idx
print(f"交易诊断: benchmark 日收益率已计算，共 {len(_diag_benchmark_daily)} 个交易日")

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
