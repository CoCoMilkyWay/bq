"""
股票池与过滤因子模块

提供:
- FILTER_NAMES: 过滤因子名称列表
- SW2021_ALL_INDUSTRIES: 申万2021行业列表
- load_interval_filter(): 加载单个 interval 类型过滤因子
- load_all_filter_intervals(): 加载所有过滤因子的合并 intervals
- apply_filter_intervals(): 向量化应用过滤区间
- prepare_filter_states(): 准备过滤因子状态（用于回测）
- get_basic_pool_sql(): 获取基础股票池 SQL

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
    数据源: bigquant cn_stock_prefactors_community
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
"""

import json
from pathlib import Path

import pandas as pd

FILTER_DIR = Path(__file__).resolve().parent

UNIVERSE_SIZE = 80

FILTER_NAMES = [
    "profit_st",
    "revenue_st",
    "risk_warning",
    "trading_st",
    "dividend_st",
    "new_listing",
]

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


def load_interval_filter(name: str, start_date: str, end_date: str):
    """
    加载单个 interval 类型过滤因子
    返回: {"name", "kind": "interval", "data": DataFrame} 或 None
    """
    indicator_path = FILTER_DIR / name / "indicator.json"
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


def load_all_filter_intervals(start_date: str, end_date: str) -> dict:
    """
    加载所有过滤因子的合并 intervals
    返回: {instrument: [(start_int, end_int), ...]}
    """
    start_int = int(start_date.replace("-", ""))
    end_int = int(end_date.replace("-", ""))
    intervals_by_inst = {}

    for name in FILTER_NAMES:
        indicator_path = FILTER_DIR / name / "indicator.json"
        if not indicator_path.exists():
            continue

        raw_rows = json.loads(indicator_path.read_text())
        assert isinstance(raw_rows, list)

        for item in raw_rows:
            assert isinstance(item, dict) and len(item) == 1
            instrument, intervals = next(iter(item.items()))
            for interval in intervals:
                s, e = interval
                if e < start_int or s > end_int:
                    continue
                if instrument not in intervals_by_inst:
                    intervals_by_inst[instrument] = []
                intervals_by_inst[instrument].append((s, e))

    return intervals_by_inst


def apply_filter_intervals(df: pd.DataFrame, intervals_by_inst: dict) -> pd.DataFrame:
    """向量化应用过滤区间"""
    df = df.copy()
    df["date_int"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["filtered"] = False

    for inst, intervals in intervals_by_inst.items():
        inst_mask = df["instrument"] == inst
        if not inst_mask.any():
            continue
        for s, e in intervals:
            interval_mask = inst_mask & (df["date_int"] >= s) & (df["date_int"] <= e)
            df.loc[interval_mask, "filtered"] = True

    result = df[~df["filtered"]].drop(columns=["date_int", "filtered"])
    return result


def prepare_filter_states(start_date: str, end_date: str, trading_dates: list) -> list:
    """
    准备过滤因子状态（用于回测）
    trading_dates: 交易日列表 (YYYY-MM-DD 格式)，用于展开 interval
    返回: list of {"name", "kind", "data", "filter_set"}
    """
    states = []
    state_names = set()
    trading_date_ints = sorted(int(d.replace("-", "")) for d in trading_dates)

    for filter_name in FILTER_NAMES:
        state = load_interval_filter(filter_name, start_date, end_date)
        if state is None:
            continue
        assert isinstance(state, dict), f"invalid state type: {filter_name}"
        assert {"name", "kind", "data"}.issubset(state.keys()), f"invalid state keys: {filter_name}"
        assert isinstance(state["name"], str) and state["name"], f"invalid state name: {filter_name}"
        assert state["name"] not in state_names, f"duplicated factor name: {state['name']}"
        assert state["kind"] in {"interval", "daily"}, f"invalid state kind: {state['kind']}"
        assert isinstance(state["data"], pd.DataFrame), f"invalid state data: {state['name']}"

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


def get_basic_pool_sql(sw2021_industries: list = None) -> str:
    """
    获取基础股票池 SQL
    sw2021_industries: 申万行业列表，默认使用 SW2021_ALL_INDUSTRIES
    """
    from bigmodule import M  # pyright: ignore[reportMissingImports]

    if sw2021_industries is None:
        sw2021_industries = SW2021_ALL_INDUSTRIES

    m1 = M.cn_stock_basic_selector.v7(
        exchanges=["上交所", "深交所"],
        list_sectors=["主板", "创业板", "科创板"],
        indexes=[],
        st_statuses=["正常"],
        margin_tradings=["两融标的", "非两融标的"],
        sw2021_industries=sw2021_industries,
        drop_suspended=True,
        m_name="m1"
    )
    basic_pool_sql = m1.data.read()["sql"]
    basic_pool_sql = basic_pool_sql.replace("AND ()", "")
    return basic_pool_sql


def get_universe_pool(
    start_date: str,
    end_date: str,
    universe_size: int,
    extra_fields: list = None,
    sw2021_industries: list = None,
    apply_filters: bool = True,
) -> pd.DataFrame:
    """
    获取每日股票池（按市值排序 + 过滤因子）
    
    参数:
        start_date: 开始日期
        end_date: 结束日期
        universe_size: 股票池大小（按市值升序取前N）
        extra_fields: 额外字段列表，如 ["upper_limit", "lower_limit"]
        sw2021_industries: 申万行业列表，默认使用 SW2021_ALL_INDUSTRIES
        apply_filters: 是否应用过滤因子
    返回:
        DataFrame[date, instrument, total_market_cap, close, ...]
    """
    import dai

    basic_pool_sql = get_basic_pool_sql(sw2021_industries)
    
    fields = ["date", "instrument", "total_market_cap", "close"]
    if extra_fields:
        fields.extend(extra_fields)
    fields_sql = ", ".join(fields)

    universe_sql = f"""
    WITH basic_pool AS (
        {basic_pool_sql}
    )
    SELECT
        {fields_sql}
    FROM cn_stock_prefactors_community
    WHERE (date, instrument) IN (SELECT date, instrument FROM basic_pool)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_market_cap ASC) <= {universe_size}
    ORDER BY date, instrument
    """

    df = dai.query(universe_sql, filters={"date": [start_date, end_date]}).df()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    print(f"基础股票池记录数: {len(df)}")

    if apply_filters:
        intervals_by_inst = load_all_filter_intervals(start_date, end_date)
        if intervals_by_inst:
            df = apply_filter_intervals(df, intervals_by_inst)
            print(f"过滤后股票池记录数: {len(df)}")

    return df
