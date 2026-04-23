from pathlib import Path

from bigmodule import M, I  # pyright: ignore[reportMissingImports]
import dai  # pyright: ignore[reportMissingImports]
import numpy as np
import pandas as pd
from tqdm import tqdm

from factor import compute_pool_factors, rank_pool_factors
from filter import get_universe_pool, UNIVERSE_SIZE

STRATEGY_DIR = Path.cwd()

BACKTEST_START_DATE = "2017-01-01"
BACKTEST_END_DATE = "2026-04-07"
HOLD_N = 40
EXIT_RATIO = 1.2
CAPITAL_BASE = 1000000
PRICE_LIMIT_EPS = 1e-4  # close vs upper/lower_limit 的浮点容差

# 固定权重因子组合 (可配置); IC 窗口仅用于诊断图, 不参与打分
IC_WINDOW_DAYS = 120
FACTOR_WEIGHTS: dict[str, float] = {
    "pe_ttm": 0.0,
    "pb": 0.27,
    "ps_ttm": 0.13,
    "pcf_ttm": 0.0,
    "roe_ttm": 0.0,
    "roa_ttm": 0.0,
    "dividend_yield": 0.13,
    "float_market_cap": 0.13,
    "total_market_cap": 0.33,
    "close": 0.0,
}
ALL_FACTOR_NAMES = list(FACTOR_WEIGHTS.keys())

"""
## 策略配置
- **股票池** (`cn_stock_basic_selector` + `cn_stock_prefactors`)
  - 基础过滤: 上交所/深交所, 主板/创业板/科创板, 排除ST, 排除停牌
  - 按 `total_market_cap` 升序取前 `UNIVERSE_SIZE` 只
  - 字段: `close` (benchmark 收益/PIT 诊断使用)
  - 股票池作为指标benchmark (过滤和排序都作为策略超额)
  - 每日重算
- **过滤因子**
  - 股票池排除过滤因子=1的标的
  - 每日重算
- **排序因子 (固定权重组合)**
  - 因子及权重由 `FACTOR_WEIGHTS` 配置 (默认 10 个因子等权 0.1)
  - 原始因子值先按 (instrument, date) 时间方向 ffill, 保证短暂数据缺失不吃 NaN; 从未有过值(如未上市)保持 NaN
  - 每日 pool 内各因子独立做 pct rank, NaN 不参与该因子截面排名
  - factor_score = Σ_f w_f * rank_{D,f}  /  Σ_f w_f * 1{rank_{D,f} 存在}
    (可用因子权重归一化, 避免因子缺失时系统性低估); 所有因子都缺则 NaN 该股不参与当日交易
  - 诊断: 保留 `IC_WINDOW_DAYS` 窗口的单因子 + 组合因子滚动 IC 可视化
- **持仓/交易**
  - 预期持仓标的数: `HOLD_N`
  - 预期仓位: 保持100%
  - 先卖出: 离开前 `HOLD_N * EXIT_RATIO` 名(包含昨日在池今日出池的标的: 无排名⇒必卖)
  - 再买入: 除开仍旧持仓的M只标的, 找出因子排名前 `HOLD_N - M` 只标的, 将剩余资金均分至新买入标的(已持仓标的不调仓)
- **交易限制** (策略层自行过滤, 不依赖 BigTrader 引擎: 引擎仅保证一字板撮合失败, 非一字板仍会成交)
  - 涨停时不会买入 (做不到): 物理约束
  - 跌停时不会卖出 (做不到): 物理约束
  - 涨停时不会卖出 (赌 T+1 超额): 策略主动意图
  - 跌停时不会买入 (避 T+1 风险): 策略主动意图
  - 判据: T 日 `close >= upper_limit - eps` / `close <= lower_limit + eps` (订单以 close 撮合, 口径自洽);
    数据源 `cn_stock_prefactors.upper_limit/lower_limit` (板块幅度差异由数据源原生处理)

过滤因子: (统一从 filter/{name}/indicator.json 加载)
**利润ST** (profit_st):
    数据源: tushare forecast + tushare disclosure
    `profit_st := 前年亏损 AND 去年预亏 AND 年报未发 AND 年报截至前(4月底)`
    - 前年亏损 = `last_parent_net < 0`
    - 去年预亏 = `type ∈ {'首亏', '续亏'}` (年报: `end_date[4:6]=='12'`)
    - 年报未发 = `date < disclosure.actual_date`
    - 年报截至 = `disclosure.actual_date ?? (end_date.year+1, 4, monthend) 次年4月月末`
**营收ST** (revenue_st):
    数据源: tushare forecast + bigquant cn_stock_financial_ttm_shift + bigquant cn_stock_basic_info
    `revenue_st := 预亏 AND TTM营收<阈值 AND 年报未发 AND 年报截至前(4月底) AND 21年后`
    - 预亏 = `type ∈ {'首亏', '续亏'}` (年报: `end_date[4:6]=='12'`)
    - TTM营收<阈值: 使用公告日可用的最新TTM营收, 阈值一般1亿, 24年起主板3亿
    - 板块判断: cn_stock_basic_info.list_sector (主板=1)
    - 年报未发/截至: 同上
    - 21年后适用: report_year >= 2021 AND ann_date >= 20210101
**交易ST** (trading_st):
    数据源: bigquant cn_stock_prefactors
    `trading_st := 连续20日(收盘价<1 OR 市值<阈值)`
    - 面值退市: 连续20个交易日收盘价 < 1元
    - 市值退市: 连续20个交易日市值 < 5亿元(主板) / 3亿元(科创板/创业板)
**分红ST** (dividend_st):
    数据源: bigquant cn_stock_dividend + cn_stock_capital + cn_stock_financial_income_general_pit
    `dividend_st := 三年累计分红 < 三年年均净利润*30% AND 三年累计分红 < 5000万`
    - 分红 = cash_before_tax * total_shares (分红预案金额)
    - 净利润 = 近2年年报归母净利润平均值 (cn_stock_financial_income_general_pit)
    - 仅主板适用
    - 科创板/创业板:研发投入满足条件可豁免(未实现)
**风险警示** (risk_warning):
    数据源: bigquant cn_stock_status
    `risk_warning := is_risk_warning = 1`
    - 风险警示公告发布后标记
**次新股** (new_listing): (17年次新股涨停潮, 不可复制, 影响回测)
    数据源: bigquant cn_stock_basic_info
    `new_listing := 上市日期距今 < 60天`
    - 过滤上市不满60天的次新股，避免涨停板无法买入的问题

排序因子:
**PE(TTM)** (`pe_ttm`): (优先:小)
    数据源: cn_stock_prefactors.pe_ttm
**PB** (`pb`): (优先:小)
    数据源: cn_stock_prefactors.pb
**PS(TTM)** (`ps_ttm`): (优先:小)
    数据源: cn_stock_prefactors.ps_ttm
**PCF(TTM)** (`pcf_ttm`): (优先:小)
    数据源: cn_stock_prefactors.pcf_net_ttm
**ROE(TTM)** (`roe_ttm`): (优先:大)
    数据源: cn_stock_prefactors.roe_avg_ttm
**ROA(TTM)** (`roa_ttm`): (优先:大)
    数据源: cn_stock_prefactors.roa_avg_ttm
**股息率** (`dividend_yield`): (优先:大)
    数据源: cn_stock_prefactors.dividend_yield_ratio
**总市值**: (优先:小)
    数据源: cn_stock_prefactors.total_market_cap
**流通市值**: (优先:小)
    数据源: cn_stock_prefactors.float_market_cap
**收盘价**: (优先:小)
    数据源: cn_stock_prefactors.close

代码编写原则:
1. 回测和实盘统一使用incremental实现, 尽量共享代码和逻辑
2. 每个因子的实现应该尽量独立定义在代码最前, 不要和后面的框架耦合

"""


def _per_date_pearson_ic(
    merged: pd.DataFrame,
    factor_cols: list[str],
    ret_col: str = "fwd_ret",
) -> pd.DataFrame:
    """
    向量化 per-date Pearson IC: 对每个因子 col, 按 date 分组算 corr(col, ret_col),
    每因子用各自非空子集 (NaN 不参与). 通过一次 groupby.sum 获取六元组 (n, Σx, Σy, Σxy, Σx², Σy²)
    再代入闭式公式, 避免 Python 层按日迭代.
    返回: DataFrame, index=date, columns=factor_cols (值为 IC, 有效样本<2 或 std=0 ⇒ NaN)
    """
    rv = merged[ret_col].to_numpy(dtype=np.float64)
    date_arr = merged["date"].to_numpy()
    series_list: list[pd.Series] = []
    for col in factor_cols:
        fv = merged[col].to_numpy(dtype=np.float64)
        valid = ~np.isnan(fv) & ~np.isnan(rv)
        x = np.where(valid, fv, 0.0)
        y = np.where(valid, rv, 0.0)
        tmp = pd.DataFrame({
            "date": date_arr,
            "n": valid.astype(np.float64),
            "sx": x,
            "sy": y,
            "sxy": x * y,
            "sx2": x * x,
            "sy2": y * y,
        })
        agg = tmp.groupby("date", sort=True).sum()
        n = agg["n"].to_numpy()
        sx, sy = agg["sx"].to_numpy(), agg["sy"].to_numpy()
        sxy = agg["sxy"].to_numpy()
        sx2, sy2 = agg["sx2"].to_numpy(), agg["sy2"].to_numpy()
        num = n * sxy - sx * sy
        denx = np.maximum(n * sx2 - sx * sx, 0.0)
        deny = np.maximum(n * sy2 - sy * sy, 0.0)
        den = np.sqrt(denx * deny)
        ic = np.where((n >= 2) & (den > 0), num / den, np.nan)
        series_list.append(pd.Series(ic, index=agg.index, name=col))
    return pd.concat(series_list, axis=1)


def compute_fixed_weight_factor_score(
    factor_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    daily_return_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    固定权重合成 factor_score.

    流程:
        1. 原始因子值按 (instrument, date) 时间方向 ffill (短暂缺失继承前值, PIT 安全)
           未上市/从未有值的 NaN 保持 NaN
        2. pool 内每因子独立 pct rank 到 [0,1], NaN 不参与该因子截面排名
        3. factor_score = Σ_f w_f * rank_{D,f} / Σ_f w_f * 1{rank_{D,f} 存在}
           (可用因子权重归一化; 全因子缺 ⇒ NaN, 不参与当日交易)
        4. 诊断 (不参与打分): 每日单因子 IC + 组合因子 rank-IC, 再做 IC_WINDOW_DAYS 滑窗均值用于可视化
    """
    f = factor_df[["date", "instrument"] + ALL_FACTOR_NAMES].sort_values(
        ["instrument", "date"]
    ).copy()
    f[ALL_FACTOR_NAMES] = f.groupby("instrument")[ALL_FACTOR_NAMES].ffill()
    ranked = rank_pool_factors(f, ALL_FACTOR_NAMES)

    score_num = np.zeros(len(ranked), dtype=np.float64)
    score_den = np.zeros(len(ranked), dtype=np.float64)
    for fn, w in FACTOR_WEIGHTS.items():
        val = ranked[fn].values
        avail = ~np.isnan(val)
        score_num += np.where(avail, val * w, 0.0)
        score_den += np.where(avail, w, 0.0)
    score = np.where(score_den > 0, score_num / score_den, np.nan)
    ranked["factor_score"] = score
    score_df = ranked[["date", "instrument", "factor_score"]]

    # ============ 诊断: 单因子 + 组合因子滚动 IC (仅用于可视化, 不参与打分) ============
    ret_df = daily_return_df.sort_values(["instrument", "date"]).copy()
    ret_df["fwd_ret"] = ret_df.groupby("instrument")["daily_return"].shift(-1)
    ret_df = ret_df[ret_df["date"] <= factor_df["date"].max()][
        ["date", "instrument", "fwd_ret"]
    ]

    merged = ranked.merge(ret_df, on=["date", "instrument"], how="left")
    merged["_score_rank"] = merged.groupby("date", sort=False)["factor_score"].transform(
        lambda s: s.rank(method="average", pct=True)
    )
    ic_wide = _per_date_pearson_ic(merged, ALL_FACTOR_NAMES + ["_score_rank"])
    ic_df = (
        ic_wide.rename(columns={"_score_rank": "combined"})
        .reset_index()
        .sort_values("date")
        .reset_index(drop=True)
    )

    roll_cols = ALL_FACTOR_NAMES + ["combined"]
    full_ic_roll = ic_df[roll_cols].rolling(IC_WINDOW_DAYS, min_periods=1).mean()
    full_ic_roll.insert(0, "date", ic_df["date"].values)
    global _diag_rolling_ic_df
    _diag_rolling_ic_df = full_ic_roll

    print(f"固定权重因子组合: {len(ALL_FACTOR_NAMES)} 个因子, 权重和 = {sum(FACTOR_WEIGHTS.values()):.3f}")
    for fn, w in FACTOR_WEIGHTS.items():
        print(f"    {fn:20s}: {w:.3f}")

    result = universe_df.merge(score_df, on=["date", "instrument"], how="left")
    return result


def compute_limit_flags(
    instruments: np.ndarray,
    closes: np.ndarray,
    upper_limits: np.ndarray,
    lower_limits: np.ndarray,
) -> tuple[set, set]:
    """
    计算 T 日涨停/跌停标的集合 (close 与 upper/lower_limit 比较, eps 容差)
    tradable 股票的 upper/lower_limit 必须非空 (停牌/未上市不应进入 tradable pool)
    """
    assert not np.isnan(closes).any(), "close has NaN in tradable pool"
    assert not np.isnan(upper_limits).any(), "upper_limit has NaN in tradable pool"
    assert not np.isnan(lower_limits).any(), "lower_limit has NaN in tradable pool"
    up_hit = closes >= upper_limits - PRICE_LIMIT_EPS
    down_hit = closes <= lower_limits + PRICE_LIMIT_EPS
    return set(instruments[up_hit]), set(instruments[down_hit])


def build_target_on_day(instruments, ranking_scores):
    """
    Per-bar 计算，与实盘共享逻辑
    参数:
        instruments: np.ndarray 或 list
        ranking_scores: np.ndarray 或 list, 分数越大越优
    返回:
        top_n_instruments: set
        top_exit_instruments: set
        rank_map: {instrument: rank}
    """
    pairs = list(zip(instruments, ranking_scores))
    if not pairs:
        return set(), set(), {}

    pairs.sort(key=lambda x: x[1], reverse=True)
    rank_map = {inst: idx + 1 for idx, (inst, _) in enumerate(pairs)}
    top_n_instruments = {inst for inst, _ in pairs[:HOLD_N]}
    exit_threshold = int(HOLD_N * EXIT_RATIO)
    top_exit_instruments = {inst for inst, _ in pairs[:exit_threshold]}
    return top_n_instruments, top_exit_instruments, rank_map


def bt_init(context):
    from bigtrader.finance.commission import ( # pyright: ignore
        PerOrder,
    )

    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    context.data["date"] = context.data["date"].dt.strftime("%Y-%m-%d")

    # 预处理 universe: (instruments, ranking_scores) 元组，避免 DataFrame 操作
    # 同时预处理当日涨停/跌停集合, 用于买入/卖出过滤
    context.universe_by_date = {}
    context.limit_flags_by_date = {}
    for date, day_df in context.data.groupby("date", sort=False):
        valid_day_df = day_df.loc[day_df["factor_score"].notna()]
        instruments_arr = valid_day_df["instrument"].values
        context.universe_by_date[date] = (
            instruments_arr,
            valid_day_df["factor_score"].values,
        )
        context.limit_flags_by_date[date] = compute_limit_flags(
            instruments_arr,
            valid_day_df["close"].values,
            valid_day_df["upper_limit"].values,
            valid_day_df["lower_limit"].values,
        )

    assert (
        len(context.universe_by_date) > 0
    ), "bigquant universe is empty in backtest range"
    universe_dates = sorted(context.universe_by_date.keys())
    assert (
        universe_dates[0] <= BACKTEST_END_DATE
    ), "bigquant coverage starts after backtest end"
    assert (
        universe_dates[-1] >= BACKTEST_START_DATE
    ), "bigquant coverage ends before backtest start"

    context.progress_total_days = len(context.universe_by_date)
    context.progress_done_days = 0
    context.progress_bar = tqdm(
        total=context.progress_total_days, desc="backtest", unit="day"
    )

    # 交易诊断初始化
    context.trade_diag = {
        "benchmark_daily_return": _diag_benchmark_daily,
        "trading_dates": _diag_trading_dates,
        "rolling_ic_df": _diag_rolling_ic_df,
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
    rank_map,
    up_limit_set,
    down_limit_set,
):
    """
    Per-day 交易决策，回测与实盘共享
    涨跌停过滤在策略层完成 (引擎仅保证一字板, 非一字板仍会成交):
        卖出侧: 排除涨停(不想卖, 赌 T+1 超额) + 跌停(卖不出去, 物理)
        买入侧: 排除涨停(买不到, 物理)     + 跌停(不想买, 避 T+1 风险)
    未能卖出的持仓继续占着仓位, 买入空位相应减少; 买入过滤不足则留现金(不扩大 candidate 池).
    参数:
        holding_instruments: set, 当前持仓标的
        top_n_instruments: set, 排名前 HOLD_N 的标的
        top_exit_instruments: set, 排名前 HOLD_N * EXIT_RATIO 的标的 (无排名⇒必卖)
        rank_map: {instrument: rank}, 排名越小越优
        up_limit_set: set, 当日涨停集合
        down_limit_set: set, 当日跌停集合
    返回:
        to_sell: list, 需要卖出的标的
        to_buy: list, 需要买入的标的
    """
    to_sell = [
        inst for inst in holding_instruments
        if inst not in top_exit_instruments
        and inst not in up_limit_set
        and inst not in down_limit_set
    ]
    remaining_holding = holding_instruments - set(to_sell)
    slots_available = HOLD_N - len(remaining_holding)
    if slots_available > 0:
        candidates = sorted(
            (inst for inst in (top_n_instruments - remaining_holding)
             if inst not in up_limit_set and inst not in down_limit_set),
            key=lambda inst: rank_map[inst],
        )
        to_buy = candidates[:slots_available]
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
    instruments, ranking_scores = universe_today
    limit_flags_today = context.limit_flags_by_date.get(trade_date)
    assert limit_flags_today is not None, f"no limit flags for {trade_date}"
    up_limit_set, down_limit_set = limit_flags_today

    top_n_instruments, top_exit_instruments, rank_map = build_target_on_day(
        instruments, ranking_scores
    )
    holding_instruments = set(context.get_account_positions().keys())

    to_sell, to_buy = decide_trades_on_day(
        holding_instruments,
        top_n_instruments,
        top_exit_instruments,
        rank_map,
        up_limit_set,
        down_limit_set,
    )

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
            "buy_value": float(trade.filled_money),
            "portfolio_value_at_open": float(context.portfolio.portfolio_value),
        }
    elif direction == "2":  # SELL
        open_rec = diag["open_records"].pop(inst, None)
        if open_rec is None:
            return
        open_date_str = open_rec["open_date"]
        open_date_str = f"{open_date_str[:4]}-{open_date_str[4:6]}-{open_date_str[6:8]}"
        close_date_str = str(trade.trade_date)
        close_date_str = (
            f"{close_date_str[:4]}-{close_date_str[4:6]}-{close_date_str[6:8]}"
        )
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

        buy_value = open_rec["buy_value"]
        pv_at_open = open_rec["portfolio_value_at_open"]
        position_pct = buy_value / pv_at_open if pv_at_open > 0 else 0.0

        diag["closed_trades"].append(
            {
                "instrument": inst,
                "open_date": open_date_str,
                "close_date": close_date_str,
                "holding_days": holding_days,
                "total_return": total_return,
                "daily_return": daily_return,
                "daily_benchmark": daily_benchmark,
                "daily_excess": daily_excess,
                "buy_value": buy_value,
                "portfolio_value_at_open": pv_at_open,
                "position_pct": position_pct,
            }
        )


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
    filtered_trades = [t for t in closed_trades if t["total_return"] <= -0.10]
    sorted_trades = sorted(filtered_trades, key=lambda x: x["daily_excess"])
    print(f"\n========== 交易诊断: 总跌幅>10%中超额最差的 30 笔交易 ==========")
    print(
        f"{'标的':<12} {'开仓日期':<12} {'平仓日期':<12} {'持仓天数':>8} {'总收益%':>10} {'日均收益%':>10} {'日均基准%':>10} {'日均超额%':>10}"
    )
    print("-" * 100)
    for t in sorted_trades[:30]:
        print(
            f"{t['instrument']:<12} {t['open_date']:<12} {t['close_date']:<12} {t['holding_days']:>8} {t['total_return']*100:>10.2f} {t['daily_return']*100:>10.4f} {t['daily_benchmark']*100:>10.4f} {t['daily_excess']*100:>10.4f}"
        )
    print(f"========== 交易诊断结束，共 {len(closed_trades)} 笔已平仓交易 ==========")

    # ========== 收益率分布图 (plotly) ==========
    import plotly.graph_objects as go
    from scipy.stats import gaussian_kde

    returns_pct = np.array([t["total_return"] for t in closed_trades], dtype=np.float64) * 100
    n = len(returns_pct)
    lo_pct, hi_pct = np.percentile(returns_pct, [0.5, 99.5])
    clipped_pct = returns_pct[(returns_pct >= lo_pct) & (returns_pct <= hi_pct)]
    kde = gaussian_kde(clipped_pct, bw_method="scott")
    xs_pct = np.linspace(lo_pct, hi_pct, 512)
    ys = kde(xs_pct)

    mean_r = float(returns_pct.mean())
    median_r = float(np.median(returns_pct))

    dist_fig = go.Figure()
    dist_fig.add_trace(
        go.Histogram(
            x=clipped_pct,
            histnorm="probability density",
            nbinsx=80,
            marker=dict(color="steelblue", line=dict(color="white", width=0.5)),
            opacity=0.45,
            name=f"histogram (n={n})",
        )
    )
    dist_fig.add_trace(
        go.Scatter(
            x=xs_pct, y=ys, mode="lines",
            line=dict(color="crimson", width=2.5),
            name="KDE (gaussian, scott)",
        )
    )
    dist_fig.add_vline(x=0, line=dict(color="black", width=1, dash="dash"), opacity=0.6)
    dist_fig.add_vline(
        x=mean_r, line=dict(color="darkorange", width=1.5, dash="dashdot"),
        annotation_text=f"mean {mean_r:.2f}%", annotation_position="top",
    )
    dist_fig.add_vline(
        x=median_r, line=dict(color="darkgreen", width=1.5, dash="dot"),
        annotation_text=f"median {median_r:.2f}%", annotation_position="bottom",
    )
    dist_fig.update_layout(
        title=f"Per-trade return distribution (n={n}, clipped to [P0.5, P99.5])",
        xaxis_title="per-trade total return (%)",
        yaxis_title="density",
        barmode="overlay",
        template="plotly_white",
        height=520,
        legend=dict(x=0.99, y=0.99, xanchor="right", yanchor="top"),
    )
    dist_fig.show()

    # ========== 滚动 IC 图 (plotly): 各单因子 + 组合因子 ==========
    ic_roll = diag["rolling_ic_df"]
    assert ic_roll is not None and not ic_roll.empty, "rolling_ic_df 未准备好"

    ic_fig = go.Figure()
    for f in ALL_FACTOR_NAMES:
        ic_fig.add_trace(
            go.Scatter(
                x=ic_roll["date"], y=ic_roll[f], mode="lines",
                line=dict(width=1.2),
                opacity=0.85,
                name=f,
            )
        )
    ic_fig.add_trace(
        go.Scatter(
            x=ic_roll["date"], y=ic_roll["combined"], mode="lines",
            line=dict(color="black", width=2.8),
            name="combined (factor_score rank-IC)",
        )
    )
    ic_fig.add_hline(y=0, line=dict(color="gray", width=1, dash="dash"), opacity=0.6)
    ic_fig.update_layout(
        title=f"Rolling IC ({IC_WINDOW_DAYS}-day, unshifted) — single factors + combined",
        xaxis_title="date",
        yaxis_title="rolling IC",
        yaxis=dict(range=[-0.1, 0.1]),
        template="plotly_white",
        height=560,
        hovermode="x unified",
        legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top"),
    )
    ic_fig.show()


# ==================== 模块链 ====================
# 1. 获取股票池（不应用过滤因子，过滤在回测时动态应用）
universe_df = get_universe_pool(
    start_date=BACKTEST_START_DATE,
    end_date=BACKTEST_END_DATE,
    universe_size=UNIVERSE_SIZE,
    extra_fields=["upper_limit", "lower_limit"],
)
assert {"date", "instrument", "total_market_cap", "close", "upper_limit", "lower_limit"}.issubset(universe_df.columns)

factor_df = compute_pool_factors(
    pool_name=f"smallcap{UNIVERSE_SIZE}",
    pool_df=universe_df[["date", "instrument"]],
    start_date=BACKTEST_START_DATE,
    end_date=BACKTEST_END_DATE,
    factor_names=ALL_FACTOR_NAMES,
    factor_weights=None,
)

# 拉 daily_return: 回测期 + 末尾多15天 (给 fwd_ret shift(-1) 留数据); 不限 pool
_ret_query_end = (
    pd.to_datetime(BACKTEST_END_DATE) + pd.Timedelta(days=15)
).strftime("%Y-%m-%d")
daily_return_df = dai.query(
    "SELECT date, instrument, daily_return FROM cn_stock_prefactors ORDER BY instrument, date",
    filters={"date": [BACKTEST_START_DATE, _ret_query_end]},
).df()
daily_return_df["date"] = pd.to_datetime(daily_return_df["date"]).dt.normalize()

universe_df = compute_fixed_weight_factor_score(factor_df, universe_df, daily_return_df)
score_coverage = universe_df["factor_score"].notna().mean()
print(f"因子分数覆盖率：{score_coverage:.2%}")
assert score_coverage > 0, "factor_score coverage is zero"

stock_data_ds = dai.DataSource.write_bdb(universe_df)

# ==================== 交易诊断 (独立模块，未来可删除) ====================
# benchmark = D 日 pool 内等权复权日收益, 与 compute_dynamic_factor_score 的 fwd_ret 同源
_diag_pool_ret = universe_df[["date", "instrument"]].merge(
    daily_return_df, on=["date", "instrument"], how="left"
)
_diag_pool_ret["date_str"] = _diag_pool_ret["date"].dt.strftime("%Y-%m-%d")
_diag_trading_dates = sorted(_diag_pool_ret["date_str"].unique().tolist())
_diag_benchmark_daily = (
    _diag_pool_ret.groupby("date_str")["daily_return"].mean().to_dict()
)
del _diag_pool_ret
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
    m_name="m5",
)
