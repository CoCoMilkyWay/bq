"""
因子分析：在策略股票池上测试 factor.py 中的因子

使用方式:
    python analysis.py
"""

import numpy as np
import pandas as pd
import dai

from factor import FACTOR_NAMES, compute_pool_factors
from filter import get_universe_pool, UNIVERSE_SIZE

START_DATE = "2017-01-01"
END_DATE = "2026-04-07"
GROUP_NUM = 5
ANALYSIS_FACTOR_NAMES = [
    "pe_ttm",
    "pb",
    "ps_ttm",
    "pcf_ttm",
    "roe_ttm",
    "roa_ttm",
    "dividend_yield",
    "total_market_cap",
    "float_market_cap",
    "close",
]
assert set(ANALYSIS_FACTOR_NAMES).issubset(set(FACTOR_NAMES)), "analysis factors not in FACTOR_NAMES"


# ==================== 因子数据获取 ====================


def get_factors_in_pool(pool_df: pd.DataFrame, pool_name: str = f"smallcap{UNIVERSE_SIZE}") -> pd.DataFrame:
    """获取股票池内的因子数据"""
    factor_names = ANALYSIS_FACTOR_NAMES
    start_date = pool_df["date"].min().strftime("%Y-%m-%d")
    end_date = pool_df["date"].max().strftime("%Y-%m-%d")
    factors_df = compute_pool_factors(
        pool_name=pool_name,
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
    获取 T+1 日收益率（使用预计算的 daily_return，已复权）
    返回: DataFrame[date, instrument, fwd_ret]
    """
    sql = """
    SELECT
        date,
        instrument,
        m_lead(daily_return, 1) AS fwd_ret
    FROM cn_stock_prefactors_community
    ORDER BY instrument, date
    """
    start_date = pool_df["date"].min().strftime("%Y-%m-%d")
    pool_end_date = pool_df["date"].max().strftime("%Y-%m-%d")
    end_date = (pool_df["date"].max() + pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    ret_df = dai.query(sql, filters={"date": [start_date, end_date]}).df()
    ret_df["date"] = pd.to_datetime(ret_df["date"]).dt.normalize()
    ret_df = ret_df.loc[ret_df["date"] <= pd.to_datetime(pool_end_date)]
    return ret_df


# ==================== 因子分析 ====================

def calc_ic(group_df: pd.DataFrame, factor_col: str) -> float:
    """计算单日 Rank IC (Spearman)"""
    valid = group_df[[factor_col, "fwd_ret"]].dropna()
    if len(valid) < 10:
        return np.nan
    return valid[[factor_col, "fwd_ret"]].corr(method="spearman").iloc[0, 1]


def calc_group_returns(group_df: pd.DataFrame, factor_col: str, group_num: int) -> pd.Series:
    """计算单日分组收益，返回 Series[Q1..Q5] = mean_ret"""
    valid = group_df[[factor_col, "fwd_ret"]].dropna(subset=[factor_col]).copy()
    if len(valid) < group_num:
        return pd.Series(dtype=float)

    # 不可交易/缺失收益按惩罚收益处理: gross return=1, 即净收益率=0
    valid["fwd_ret"] = valid["fwd_ret"].fillna(0.0)

    # 用截面排序分桶，避免 qcut 在重复边界时丢层或报错
    rank_asc = valid[factor_col].rank(method="first", ascending=True)
    group_idx = ((rank_asc - 1) * group_num / len(valid)).astype(int).clip(upper=group_num - 1)
    valid["group"] = "Q" + (group_idx + 1).astype(str)

    labels = [f"Q{i+1}" for i in range(group_num)]
    grouped = valid.groupby("group")["fwd_ret"].mean().reindex(labels)
    return grouped


def analyze_factor(df: pd.DataFrame, factor_col: str, group_num: int = GROUP_NUM) -> dict:
    """
    单因子分析
    返回: {ic_mean, ic_std, icir, t_stat, ic_positive_ratio, group_ret_df, ic_df, rolling_ic}
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

    ic_df = pd.DataFrame(ic_list).dropna().set_index("date")
    ic_series = ic_df["ic"]
    n = len(ic_series)

    ic_mean = ic_series.mean()
    ic_std = ic_series.std()
    icir = ic_mean / ic_std if ic_std > 0 else 0
    t_stat = ic_mean / (ic_std / np.sqrt(n)) if ic_std > 0 and n > 0 else 0
    ic_positive_ratio = (ic_series > 0).mean()

    rolling_ic = pd.DataFrame(index=ic_series.index)
    rolling_ic["ic"] = ic_series
    rolling_ic["rolling_6"] = ic_series.rolling(6, min_periods=1).mean()
    rolling_ic["rolling_12"] = ic_series.rolling(12, min_periods=1).mean()
    rolling_ic_std_12 = ic_series.rolling(12, min_periods=1).std()
    rolling_ic["rolling_icir_12"] = rolling_ic["rolling_12"] / rolling_ic_std_12.replace(0, np.nan)

    group_ret_df = pd.DataFrame(group_ret_list)
    if not group_ret_df.empty:
        group_ret_df = group_ret_df.set_index("date").sort_index()
        if f"Q{group_num}" in group_ret_df.columns and "Q1" in group_ret_df.columns:
            group_ret_df[f"Q{group_num}-Q1"] = group_ret_df[f"Q{group_num}"] - group_ret_df["Q1"]

    return {
        "ic_mean": ic_mean,
        "ic_std": ic_std,
        "icir": icir,
        "t_stat": t_stat,
        "ic_positive_ratio": ic_positive_ratio,
        "ic_df": ic_df,
        "rolling_ic": rolling_ic,
        "group_ret_df": group_ret_df,
    }


def calc_benchmark_returns(df: pd.DataFrame) -> pd.Series:
    """计算股票池等权基准收益"""
    return df.groupby("date")["fwd_ret"].mean()


def analyze_all_factors(df: pd.DataFrame) -> dict:
    """分析所有因子"""
    factor_names = ANALYSIS_FACTOR_NAMES
    results = {}

    benchmark = calc_benchmark_returns(df)

    for factor_name in factor_names:
        print(f"分析因子: {factor_name}")
        result = analyze_factor(df, factor_name)

        if not result["group_ret_df"].empty:
            top_col = f"Q{GROUP_NUM}"
            if top_col in result["group_ret_df"].columns:
                top_ret = result["group_ret_df"][top_col]
                aligned_bm = benchmark.reindex(top_ret.index)
                excess_ret = (top_ret - aligned_bm).mean() * 252
                result["top_group_excess_annual"] = excess_ret
                result["top_group_annual"] = top_ret.mean() * 252
            else:
                result["top_group_excess_annual"] = np.nan
                result["top_group_annual"] = np.nan

            long_short_col = f"Q{GROUP_NUM}-Q1"
            if long_short_col in result["group_ret_df"].columns:
                result["long_short_annual"] = result["group_ret_df"][long_short_col].mean() * 252
            else:
                result["long_short_annual"] = np.nan
        else:
            result["top_group_excess_annual"] = np.nan
            result["top_group_annual"] = np.nan
            result["long_short_annual"] = np.nan

        results[factor_name] = result

    return results


def calc_comprehensive_score(r: dict) -> float:
    """计算综合评分: ICIR × 10 + IC>0占比 × 5"""
    return r["icir"] * 10 + r["ic_positive_ratio"] * 5


def judge_factor_validity(r: dict) -> str:
    """判定因子有效性"""
    if abs(r["t_stat"]) >= 2 and abs(r["icir"]) >= 0.3:
        return "有效"
    elif abs(r["t_stat"]) >= 2 or abs(r["icir"]) >= 0.3:
        return "弱效"
    else:
        return "无效"


def print_summary(results: dict):
    """打印因子分析汇总（含综合评分排名）"""
    print("\n" + "=" * 120)
    print("因子分析汇总")
    print("=" * 120)
    print(f"{'因子':<15} {'IC均值':>8} {'ICIR':>8} {'IC>0%':>8} {'t统计量':>8} "
          f"{'多头年化':>10} {'多空年化':>10} {'综合评分':>10} {'判定':>6}")
    print("-" * 120)

    scored = []
    for name, r in results.items():
        score = calc_comprehensive_score(r)
        validity = judge_factor_validity(r)
        scored.append((name, r, score, validity))

    scored.sort(key=lambda x: x[2], reverse=True)

    for name, r, score, validity in scored:
        top_annual = r.get("top_group_annual", np.nan)
        long_short = r.get("long_short_annual", np.nan)
        top_str = f"{top_annual:>10.2%}" if not np.isnan(top_annual) else f"{'N/A':>10}"
        ls_str = f"{long_short:>10.2%}" if not np.isnan(long_short) else f"{'N/A':>10}"
        print(f"{name:<15} {r['ic_mean']:>8.4f} {r['icir']:>8.4f} {r['ic_positive_ratio']:>8.1%} {r['t_stat']:>8.2f} "
              f"{top_str} {ls_str} {score:>10.2f} {validity:>6}")

    print("=" * 120)
    print("判定标准: |t|>=2 且 |ICIR|>=0.3 为有效; 满足其一为弱效; 均不满足为无效")


# ==================== 因子相关性 ====================

def calc_factor_correlation(df: pd.DataFrame) -> pd.DataFrame:
    """计算因子间相关性矩阵（截面内排序标准化到[0,1]后计算）"""
    factor_names = ANALYSIS_FACTOR_NAMES
    factor_cols = [c for c in factor_names if c in df.columns]

    ranked = df.groupby("date")[factor_cols].rank(pct=True)
    corr_matrix = ranked.corr(method="spearman")
    return corr_matrix

def plot_correlation_heatmap(corr_df: pd.DataFrame):
    """绘制相关性矩阵热力图（颜色按绝对值，标注数值）"""
    import plotly.graph_objects as go  # pyright: ignore[reportMissingImports]

    factors = corr_df.columns.tolist()
    z = corr_df.values
    text = [[f"{v:.2f}" for v in row] for row in z]

    fig = go.Figure(data=go.Heatmap(
        z=z,
        x=factors,
        y=factors,
        text=text,
        texttemplate="%{text}",
        colorscale="RdBu_r",
        zmid=0,
        zmin=-1,
        zmax=1,
    ))
    fig.update_layout(
        title="因子相关性矩阵 (Spearman)",
        xaxis=dict(tickangle=45),
        width=700,
        height=600,
    )
    fig.show()


def plot_factor_layers(results: dict):
    """为每个因子绘制分层累积净值曲线 (Q1-Q5 + 多空)"""
    import plotly.graph_objects as go  # pyright: ignore[reportMissingImports]
    from plotly.subplots import make_subplots  # pyright: ignore[reportMissingImports]

    factor_names = [k for k, r in results.items() if not r["group_ret_df"].empty]
    if not factor_names:
        return

    n = len(factor_names)
    fig = make_subplots(rows=n, cols=1, subplot_titles=factor_names, vertical_spacing=0.05)

    for i, factor_name in enumerate(factor_names, 1):
        group_ret_df = results[factor_name]["group_ret_df"]
        layer_cols = [c for c in group_ret_df.columns if c.startswith("Q") and "-" not in c]
        nav_df = (1 + group_ret_df[layer_cols].fillna(0)).cumprod()
        for col in nav_df.columns:
            fig.add_trace(go.Scatter(x=nav_df.index, y=nav_df[col], mode="lines", name=col, showlegend=(i == 1)), row=i, col=1)

    fig.update_layout(height=300 * n, title_text="因子分层净值")
    fig.show()


def plot_ic_rolling(results: dict):
    """为每个因子绘制IC滚动分析图（IC序列 + 滚动均值）"""
    import plotly.graph_objects as go  # pyright: ignore[reportMissingImports]
    from plotly.subplots import make_subplots  # pyright: ignore[reportMissingImports]

    factor_names = [k for k, r in results.items() if r.get("rolling_ic") is not None and not r["rolling_ic"].empty]
    if not factor_names:
        return

    n = len(factor_names)
    fig = make_subplots(rows=n, cols=1, subplot_titles=factor_names, vertical_spacing=0.05)

    for i, factor_name in enumerate(factor_names, 1):
        rolling_ic = results[factor_name]["rolling_ic"]
        for col in ["ic", "rolling_6", "rolling_12"]:
            fig.add_trace(go.Scatter(x=rolling_ic.index, y=rolling_ic[col], mode="lines", name=col, showlegend=(i == 1)), row=i, col=1)

    fig.update_layout(height=300 * n, title_text="IC滚动分析")
    fig.show()


# ==================== 主流程 ====================

def main():
    print("=" * 80)
    print("因子分析开始")
    print(f"时间范围: {START_DATE} ~ {END_DATE}")
    print(f"股票池大小: {UNIVERSE_SIZE}, 分组数: {GROUP_NUM}")
    print("=" * 80)

    print("\n[1/5] 构建股票池...")
    pool_df = get_universe_pool(START_DATE, END_DATE, UNIVERSE_SIZE)
    daily_count = pool_df.groupby("date").size()
    print(f"每日标的数: 平均={daily_count.mean():.1f}, 最小={daily_count.min()}, 最大={daily_count.max()}")

    print("\n[2/5] 获取收益率数据...")
    ret_df = get_forward_returns(pool_df)
    pool_ret = pool_df.merge(ret_df, on=["date", "instrument"], how="left")
    pool_ret["year"] = pool_ret["date"].dt.year
    yearly_stats = pool_ret.groupby("year")["fwd_ret"].agg(["mean", "std", "min", "max", "count"])
    yearly_stats["mean"] = yearly_stats["mean"] * 100
    yearly_stats["std"] = yearly_stats["std"] * 100
    yearly_stats["min"] = yearly_stats["min"] * 100
    yearly_stats["max"] = yearly_stats["max"] * 100
    yearly_stats["annual"] = yearly_stats["mean"] * 252
    print("\n按年收益率统计 (%):")
    print(yearly_stats.round(2))
    daily_ret = pool_ret.groupby("date")["fwd_ret"].mean()
    print(f"\n总体: 日均={daily_ret.mean()*100:.3f}%, 累计净值={(1+daily_ret).prod():.1f}倍")

    print("\n[3/5] 获取因子数据...")
    factors_df = get_factors_in_pool(pool_df)
    df = factors_df.merge(ret_df, on=["date", "instrument"], how="left")

    print("\n[4/5] 因子分析...")
    results = analyze_all_factors(df)
    print_summary(results)

    print("\n[5/5] 因子相关性...")
    corr_df = calc_factor_correlation(df)
    plot_correlation_heatmap(corr_df)

    print("\n绘制分层净值图...")
    plot_factor_layers(results)

    print("\n绘制IC滚动图...")
    plot_ic_rolling(results)

    return results, corr_df


if __name__ == "__main__":
    main()
