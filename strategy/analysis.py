"""
因子分析：在策略股票池上测试 factor.py 中的因子

使用方式:
    python analysis.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import dai
import bigcharts  # pyright: ignore[reportMissingImports]

from factor import FACTOR_NAMES, build_pool_factors

STRATEGY_DIR = Path.cwd()
START_DATE = "2025-01-01"
END_DATE = "2026-04-07"
UNIVERSE_SIZE = 400
GROUP_NUM = 10

FILTER_NAMES = [
    "profit_st",
    "revenue_st",
    "risk_warning",
    "trading_st",
    "dividend_st",
]

SW2021_ALL_INDUSTRIES = [
    "基础化工", "有色金属", "建筑材料", "建筑装饰",
    "机械设备", "电子", "汽车", "家用电器", "食品饮料", "纺织服饰",
    "轻工制造", "医药生物", "公用事业", "商贸零售",
    "社会服务", "非银金融", "综合", "电力设备", "国防军工",
    "计算机", "传媒", "通信", "煤炭", "石油石化", "美容护理",
    "农林牧渔", "钢铁", "银行",
]


# ==================== 股票池构建 ====================

def load_all_filter_intervals(start_date: str, end_date: str) -> dict:
    """
    加载所有过滤因子的 interval
    返回: {instrument: [(start_int, end_int), ...]}
    """
    start_int = int(start_date.replace("-", ""))
    end_int = int(end_date.replace("-", ""))
    intervals_by_inst = {}

    for name in FILTER_NAMES:
        indicator_path = STRATEGY_DIR / "filter" / name / "indicator.json"
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
            interval_mask = inst_mask & (
                df["date_int"] >= s) & (df["date_int"] <= e)
            df.loc[interval_mask, "filtered"] = True

    result = df[~df["filtered"]].drop(columns=["date_int", "filtered"])
    return result


def get_universe_pool(start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取每日股票池（过滤后的干净池）
    返回: DataFrame[date, instrument, total_market_cap, close]
    """
    from bigmodule import M  # pyright: ignore[reportMissingImports]

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
    basic_pool_sql = m1.data.read()["sql"]
    basic_pool_sql = basic_pool_sql.replace("AND ()", "")

    universe_sql = f"""
    WITH basic_pool AS (
        {basic_pool_sql}
    )
    SELECT
        date,
        instrument,
        total_market_cap,
        close
    FROM cn_stock_prefactors_community
    WHERE (date, instrument) IN (SELECT date, instrument FROM basic_pool)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_market_cap ASC) <= {UNIVERSE_SIZE}
    ORDER BY date, instrument
    """

    df = dai.query(universe_sql, filters={"date": [start_date, end_date]}).df()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    print(f"基础股票池记录数: {len(df)}")

    intervals_by_inst = load_all_filter_intervals(start_date, end_date)
    if intervals_by_inst:
        df = apply_filter_intervals(df, intervals_by_inst)
        print(f"过滤后股票池记录数: {len(df)}")

    return df


# ==================== 因子数据获取 ====================


def get_factors_in_pool(pool_df: pd.DataFrame) -> pd.DataFrame:
    """获取股票池内的因子数据"""
    factor_names = FACTOR_NAMES
    start_date = pool_df["date"].min().strftime("%Y-%m-%d")
    end_date = pool_df["date"].max().strftime("%Y-%m-%d")
    factors_df = build_pool_factors(
        pool_df=pool_df[["date", "instrument"]],
        start_date=start_date,
        end_date=end_date,
        factor_names=factor_names,
    )
    assert len(factors_df) > 0, "factor result is empty"
    print(f"因子记录数: {len(factors_df)}")
    return factors_df


# ==================== 收益率计算 ====================

def get_forward_returns(pool_df: pd.DataFrame) -> pd.DataFrame:
    """
    计算 T+1 开盘买入 T+2 开盘卖出的收益率
    返回: DataFrame[date, instrument, fwd_ret]
    """
    sql = """
    SELECT
        date,
        instrument,
        (m_lead(open, 2) / m_lead(open, 1) - 1) AS fwd_ret
    FROM cn_stock_bar1d
    ORDER BY instrument, date
    """
    start_date = pool_df["date"].min().strftime("%Y-%m-%d")
    end_date = pool_df["date"].max().strftime("%Y-%m-%d")

    ret_df = dai.query(sql, filters={"date": [start_date, end_date]}).df()
    ret_df["date"] = pd.to_datetime(ret_df["date"]).dt.normalize()
    return ret_df


# ==================== 因子分析 ====================

def calc_ic(group_df: pd.DataFrame, factor_col: str) -> float:
    """计算单日 Rank IC (Spearman)"""
    valid = group_df[[factor_col, "fwd_ret"]].dropna()
    if len(valid) < 10:
        return np.nan
    return valid[[factor_col, "fwd_ret"]].corr(method="spearman").iloc[0, 1]


def calc_group_returns(group_df: pd.DataFrame, factor_col: str, group_num: int) -> pd.Series:
    """计算单日分组收益，返回 Series[group_id] = mean_ret"""
    valid = group_df[[factor_col, "fwd_ret"]].dropna()
    if len(valid) < group_num:
        return pd.Series(dtype=float)
    valid["group"] = pd.qcut(
        valid[factor_col], q=group_num, labels=False, duplicates="drop")
    return valid.groupby("group")["fwd_ret"].mean()


def analyze_factor(df: pd.DataFrame, factor_col: str, group_num: int = GROUP_NUM) -> dict:
    """
    单因子分析
    返回: {ic_mean, ic_std, ir, group_returns_df, ic_series}
    """
    ic_list = []
    group_ret_list = []

    for date, gdf in df.groupby("date"):
        ic = calc_ic(gdf, factor_col)
        ic_list.append({"date": date, "ic": ic})

        gret = calc_group_returns(gdf, factor_col, group_num)
        if not gret.empty:
            gret_dict = {"date": date}
            gret_dict.update(gret.to_dict())
            group_ret_list.append(gret_dict)

    ic_df = pd.DataFrame(ic_list).dropna()
    ic_mean = ic_df["ic"].mean()
    ic_std = ic_df["ic"].std()
    ir = ic_mean / ic_std if ic_std > 0 else 0

    group_ret_df = pd.DataFrame(group_ret_list)
    if not group_ret_df.empty:
        group_ret_df = group_ret_df.set_index("date").sort_index()

    return {
        "ic_mean": ic_mean,
        "ic_std": ic_std,
        "ir": ir,
        "ic_df": ic_df.set_index("date"),
        "group_ret_df": group_ret_df,
    }


def calc_benchmark_returns(df: pd.DataFrame) -> pd.Series:
    """计算股票池等权基准收益"""
    return df.groupby("date")["fwd_ret"].mean()


def analyze_all_factors(df: pd.DataFrame) -> dict:
    """分析所有因子"""
    factor_names = FACTOR_NAMES
    results = {}

    benchmark = calc_benchmark_returns(df)

    for factor_name in factor_names:
        print(f"分析因子: {factor_name}")
        result = analyze_factor(df, factor_name)

        if not result["group_ret_df"].empty:
            top_group = result["group_ret_df"].columns.max()
            top_ret = result["group_ret_df"][top_group]
            aligned_bm = benchmark.reindex(top_ret.index)
            excess_ret = (top_ret - aligned_bm).mean() * 252
            result["top_group_excess_annual"] = excess_ret
            result["top_group_annual"] = top_ret.mean() * 252
        else:
            result["top_group_excess_annual"] = np.nan
            result["top_group_annual"] = np.nan

        results[factor_name] = result

    return results


def print_summary(results: dict):
    """打印因子分析汇总"""
    print("\n" + "=" * 80)
    print("因子分析汇总")
    print("=" * 80)
    print(f"{'因子':<15} {'IC均值':>10} {'IC标准差':>10} {'IR':>10} {'多头年化':>12} {'超额年化':>12}")
    print("-" * 80)

    for name, r in results.items():
        print(f"{name:<15} {r['ic_mean']:>10.4f} {r['ic_std']:>10.4f} {r['ir']:>10.4f} "
              f"{r['top_group_annual']:>12.2%} {r['top_group_excess_annual']:>12.2%}")

    print("=" * 80)


# ==================== 因子相关性 ====================

def calc_factor_correlation(df: pd.DataFrame) -> pd.DataFrame:
    """计算因子间相关性矩阵"""
    factor_names = FACTOR_NAMES
    factor_cols = [c for c in factor_names if c in df.columns]

    corr_matrix = df[factor_cols].corr(method="spearman")
    return corr_matrix


def print_correlation_matrix(corr_df: pd.DataFrame):
    """打印相关性矩阵"""
    print("\n" + "=" * 80)
    print("因子相关性矩阵 (Spearman)")
    print("=" * 80)

    pd.set_option("display.float_format", "{:.3f}".format)
    pd.set_option("display.width", 120)
    print(corr_df)
    print("=" * 80)


def plot_factor_layers(results: dict):
    """为每个因子绘制分层累积收益曲线"""
    from bigcharts import opts  # pyright: ignore[reportMissingImports]

    charts = []
    for factor_name, r in results.items():
        group_ret_df = r["group_ret_df"]
        if group_ret_df.empty:
            continue
        group_cumret = group_ret_df.cumsum()
        group_cumret.columns = [f"G{c}" for c in group_cumret.columns]
        group_cumret = group_cumret.reset_index()
        c = bigcharts.Chart(
            data=group_cumret,
            type_="line",
            x="date",
            y=[col for col in group_cumret.columns if col != "date"],
            chart_options=dict(
                title_opts=opts.TitleOpts(
                    title=f"{factor_name} 分层收益", pos_left="center"),
            ),
        )
        charts.append(c)

    if charts:
        page = bigcharts.Chart(charts, type_="page", init_opts={"height": "400px"}).render(display=False)
        from IPython.display import display    # pyright: ignore
        display(page)


# ==================== 主流程 ====================

def main():
    print("=" * 80)
    print("因子分析开始")
    print(f"时间范围: {START_DATE} ~ {END_DATE}")
    print(f"股票池大小: {UNIVERSE_SIZE}, 分组数: {GROUP_NUM}")
    print("=" * 80)

    print("\n[1/4] 构建股票池...")
    pool_df = get_universe_pool(START_DATE, END_DATE)

    print("\n[2/4] 获取因子数据...")
    factors_df = get_factors_in_pool(pool_df)

    print("\n[3/4] 获取收益率数据...")
    ret_df = get_forward_returns(pool_df)
    df = factors_df.merge(ret_df, on=["date", "instrument"], how="left")
    print(f"合并后记录数: {len(df)}")

    print("\n[4/4] 因子分析...")
    results = analyze_all_factors(df)
    print_summary(results)

    print("\n[5/5] 因子相关性...")
    corr_df = calc_factor_correlation(df)
    print_correlation_matrix(corr_df)

    print("\n[6/6] 绘制分层图...")
    plot_factor_layers(results)

    return results, corr_df


if __name__ == "__main__":
    main()
