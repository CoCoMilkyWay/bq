"""
网格搜索因子挖掘

优化目标: 5档多空收益 (Q5/Q1)
因子处理: 截面排序 -> [0,1] 分位数 -> 加权求和 -> 再排序分档

流程:
    1. 单因子测试，筛选 top N 因子
    2. 粗粒度网格搜索 (0.3 间隔)
    3. 热点附近细粒度搜索

使用方式:
    python ga_mining.py
"""

import numpy as np
import numba
from pathlib import Path

# ==================== 配置 ====================

DATA_FILE = Path(__file__).parent / "ga_mining_data.npz"

START_DATE = "2017-01-01"
END_DATE = "2026-04-07"
GROUP_NUM = 5  # 分档数

TOP_N_FACTORS = 5  # 筛选因子数
COARSE_STEP = 0.3  # 粗搜步长
FINE_STEP = 0.1    # 细搜步长
FINE_RADIUS = 0.2  # 细搜范围 (热点 ± radius)
TOP_K_HOTSPOTS = 10  # 保留多少热点做细搜

FACTOR_NAMES_TO_USE = [
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


# ==================== 数据导出/加载 ====================

def export_data(output_path: Path = DATA_FILE) -> None:
    """
    从数据源加载数据，转换后保存为压缩文件
    """
    import pandas as pd
    from factor import read_pool_factors, ensure_pool_factors
    from filter import get_universe_pool, UNIVERSE_SIZE

    POOL_NAME = f"smallcap{UNIVERSE_SIZE}"

    print("=" * 60)
    print("导出数据")
    print("=" * 60)

    print("加载股票池...")
    pool_df = get_universe_pool(START_DATE, END_DATE, UNIVERSE_SIZE)

    print("加载因子数据...")
    ensure_pool_factors(POOL_NAME, END_DATE, FACTOR_NAMES_TO_USE, pool_df)
    factor_df = read_pool_factors(POOL_NAME, START_DATE, END_DATE, FACTOR_NAMES_TO_USE)

    print("加载收益率...")
    from analysis import get_forward_returns
    ret_df = get_forward_returns(pool_df)

    # 合并
    df = factor_df.merge(ret_df, on=["date", "instrument"], how="left")

    # 构建索引
    dates = sorted(df["date"].unique())
    date_to_idx = {d: i for i, d in enumerate(dates)}
    all_instruments = sorted(df["instrument"].unique())
    inst_to_idx = {inst: i for i, inst in enumerate(all_instruments)}

    n_dates = len(dates)
    n_stocks = len(all_instruments)
    n_factors = len(FACTOR_NAMES_TO_USE)

    # 初始化
    factors_raw = np.full((n_dates, n_stocks, n_factors), np.nan, dtype=np.float32)
    returns = np.full((n_dates, n_stocks), np.nan, dtype=np.float32)

    # 向量化索引
    d_indices = df["date"].map(date_to_idx).values
    i_indices = df["instrument"].map(inst_to_idx).values

    # 向量化填充因子
    factor_values = df[FACTOR_NAMES_TO_USE].values.astype(np.float32)
    for f_idx in range(n_factors):
        factors_raw[d_indices, i_indices, f_idx] = factor_values[:, f_idx]

    # 向量化填充收益
    returns[d_indices, i_indices] = df["fwd_ret"].values.astype(np.float32)

    # 转换为截面排名分位数 [0, 1]
    print("转换因子为截面排名分位数...")
    factor_ranks = _convert_to_rank_percentile(factors_raw)

    # 保存压缩文件
    print(f"保存到 {output_path}...")
    np.savez_compressed(
        output_path,
        factor_ranks=factor_ranks,
        returns=returns,
        factor_names=np.array(FACTOR_NAMES_TO_USE),
    )

    file_size = output_path.stat().st_size / 1024 / 1024
    print(f"数据形状: factor_ranks {factor_ranks.shape}, returns {returns.shape}")
    print(f"文件大小: {file_size:.2f} MB")
    print("导出完成")


def load_data_from_file(input_path: Path = DATA_FILE) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """
    从压缩文件加载数据
    返回:
        factor_ranks: (n_dates, n_stocks, n_factors) float32
        returns: (n_dates, n_stocks) float32
        factor_names: 因子名称列表
    """
    assert input_path.exists(), f"数据文件不存在: {input_path}"

    print(f"从 {input_path} 加载数据...")
    data = np.load(input_path)
    factor_ranks = data["factor_ranks"]
    returns = data["returns"]
    factor_names = data["factor_names"].tolist()

    print(f"数据形状: factor_ranks {factor_ranks.shape}, returns {returns.shape}")
    print(f"因子: {factor_names}")
    return factor_ranks, returns, factor_names


def _convert_to_rank_percentile(factors: np.ndarray) -> np.ndarray:
    """
    将因子值转换为截面排名分位数 [0, 1]
    factors: (n_dates, n_stocks, n_factors)
    返回: (n_dates, n_stocks, n_factors) 排名分位数
    """
    n_dates, n_stocks, n_factors = factors.shape
    ranks = np.full_like(factors, np.nan)

    for d in range(n_dates):
        for f in range(n_factors):
            col = factors[d, :, f]
            valid_mask = ~np.isnan(col)
            if valid_mask.sum() < 2:
                continue
            valid_vals = col[valid_mask]
            order = valid_vals.argsort().argsort() + 1
            percentile = (order - 1) / (len(order) - 1)
            ranks[d, valid_mask, f] = percentile

    return ranks.astype(np.float32)


# ==================== numba 评估器 ====================

@numba.jit(nopython=True, cache=True)
def _eval_single(weights: np.ndarray, factor_ranks: np.ndarray, returns: np.ndarray, group_num: int) -> float:
    """
    评估单个权重向量的多空累计收益比 (Q5/Q1)
    """
    n_dates = factor_ranks.shape[0]
    n_stocks = factor_ranks.shape[1]
    n_factors = len(weights)

    nav_top = 1.0
    nav_bottom = 1.0

    for d in range(n_dates):
        scores = np.empty(n_stocks, dtype=np.float32)
        valid_count = 0
        for i in range(n_stocks):
            s = 0.0
            all_valid = True
            for f in range(n_factors):
                v = factor_ranks[d, i, f]
                if np.isnan(v):
                    all_valid = False
                    break
                s += v * weights[f]
            if all_valid:
                scores[i] = s
                valid_count += 1
            else:
                scores[i] = np.nan

        if valid_count < group_num:
            continue

        valid_indices = np.empty(valid_count, dtype=np.int32)
        valid_scores = np.empty(valid_count, dtype=np.float32)
        vi = 0
        for i in range(n_stocks):
            if not np.isnan(scores[i]):
                valid_indices[vi] = i
                valid_scores[vi] = scores[i]
                vi += 1

        sorted_order = np.argsort(valid_scores)
        group_size = valid_count // group_num
        if group_size < 1:
            continue

        q1_start, q1_end = 0, group_size
        q5_start, q5_end = valid_count - group_size, valid_count

        ret_sum_q1, cnt_q1 = 0.0, 0
        for j in range(q1_start, q1_end):
            idx = valid_indices[sorted_order[j]]
            r = returns[d, idx]
            if not np.isnan(r):
                ret_sum_q1 += r
                cnt_q1 += 1

        ret_sum_q5, cnt_q5 = 0.0, 0
        for j in range(q5_start, q5_end):
            idx = valid_indices[sorted_order[j]]
            r = returns[d, idx]
            if not np.isnan(r):
                ret_sum_q5 += r
                cnt_q5 += 1

        day_ret_q1 = ret_sum_q1 / cnt_q1 if cnt_q1 > 0 else 0.0
        day_ret_q5 = ret_sum_q5 / cnt_q5 if cnt_q5 > 0 else 0.0

        nav_top *= (1.0 + day_ret_q5)
        nav_bottom *= (1.0 + day_ret_q1)

    if nav_bottom > 0:
        return nav_top / nav_bottom
    else:
        return 0.0


@numba.jit(nopython=True, parallel=True, cache=True)
def evaluate_batch(pop: np.ndarray, factor_ranks: np.ndarray, returns: np.ndarray, group_num: int) -> np.ndarray:
    """并行评估整个种群"""
    n_pop = pop.shape[0]
    fitness = np.empty(n_pop, dtype=np.float64)
    for i in numba.prange(n_pop):
        fitness[i] = _eval_single(pop[i], factor_ranks, returns, group_num)
    return fitness


# ==================== 网格搜索 ====================

def test_single_factors(factor_ranks: np.ndarray, returns: np.ndarray, factor_names: list[str]) -> list[tuple[str, int, float]]:
    """
    单因子测试，返回 [(因子名, 因子索引, 多空比), ...]
    """
    n_factors = len(factor_names)
    results = []
    
    for f_idx in range(n_factors):
        weights = np.zeros(n_factors, dtype=np.float32)
        weights[f_idx] = 1.0
        fitness = _eval_single(weights, factor_ranks, returns, GROUP_NUM)
        results.append((factor_names[f_idx], f_idx, fitness))
    
    results.sort(key=lambda x: x[2], reverse=True)
    return results


def generate_grid(n_factors: int, step: float) -> np.ndarray:
    """生成网格点"""
    from itertools import product
    values = np.arange(0, 1.0 + step / 2, step)
    grid = list(product(values, repeat=n_factors))
    return np.array(grid, dtype=np.float32)


def generate_fine_grid(center: np.ndarray, radius: float, step: float) -> np.ndarray:
    """在热点周围生成细粒度网格"""
    from itertools import product
    n_factors = len(center)
    ranges = []
    for c in center:
        lo = max(0.0, c - radius)
        hi = min(1.0, c + radius)
        vals = np.arange(lo, hi + step / 2, step)
        ranges.append(vals)
    grid = list(product(*ranges))
    return np.array(grid, dtype=np.float32)


def run_grid_search(factor_ranks: np.ndarray, returns: np.ndarray, factor_names: list[str]) -> tuple[np.ndarray, float, list[str]]:
    """
    多轮网格搜索
    返回: (最优权重, 最优多空比, 选中的因子名列表)
    """
    n_all_factors = len(factor_names)
    
    # Step 1: 单因子测试
    print(f"\n[Step 1] 单因子测试...")
    single_results = test_single_factors(factor_ranks, returns, factor_names)
    
    print(f"单因子多空比排名:")
    for name, idx, fitness in single_results:
        print(f"  {name:20s}: {fitness:.4f}")
    
    # 筛选 top N
    selected = single_results[:TOP_N_FACTORS]
    selected_names = [x[0] for x in selected]
    selected_indices = [x[1] for x in selected]
    
    print(f"\n选中因子: {selected_names}")
    
    # 提取选中因子的数据
    selected_ranks = factor_ranks[:, :, selected_indices]
    
    # Step 2: 粗粒度搜索
    print(f"\n[Step 2] 粗粒度网格搜索 (step={COARSE_STEP})...")
    coarse_grid = generate_grid(TOP_N_FACTORS, COARSE_STEP)
    print(f"搜索点数: {len(coarse_grid)}")
    
    coarse_fitness = evaluate_batch(coarse_grid, selected_ranks, returns, GROUP_NUM)
    
    # 找热点
    top_k_idx = np.argsort(coarse_fitness)[-TOP_K_HOTSPOTS:][::-1]
    hotspots = coarse_grid[top_k_idx]
    hotspot_fitness = coarse_fitness[top_k_idx]
    
    print(f"Top {TOP_K_HOTSPOTS} 热点:")
    for i, (w, f) in enumerate(zip(hotspots, hotspot_fitness)):
        w_str = ", ".join(f"{v:.1f}" for v in w)
        print(f"  #{i+1}: [{w_str}] -> {f:.4f}")
    
    # Step 3: 细粒度搜索
    print(f"\n[Step 3] 细粒度搜索 (step={FINE_STEP}, radius={FINE_RADIUS})...")
    
    best_weights = hotspots[0]
    best_fitness = hotspot_fitness[0]
    
    for i, center in enumerate(hotspots):
        fine_grid = generate_fine_grid(center, FINE_RADIUS, FINE_STEP)
        fine_fitness = evaluate_batch(fine_grid, selected_ranks, returns, GROUP_NUM)
        
        local_best_idx = np.argmax(fine_fitness)
        if fine_fitness[local_best_idx] > best_fitness:
            best_fitness = fine_fitness[local_best_idx]
            best_weights = fine_grid[local_best_idx].copy()
    
    print(f"细搜后最优: {best_fitness:.4f}")
    
    # 转换回全因子权重
    full_weights = np.zeros(n_all_factors, dtype=np.float32)
    for i, idx in enumerate(selected_indices):
        full_weights[idx] = best_weights[i]
    
    return full_weights, best_fitness, selected_names


# ==================== 主程序 ====================

def main():
    print("=" * 60)
    print("网格搜索因子挖掘")
    print("优化目标: 5档多空收益比 (Q5/Q1)")
    print("=" * 60)

    if not DATA_FILE.exists():
        print(f"数据文件不存在，开始生成...")
        export_data()
        print()

    factor_ranks, returns, factor_names = load_data_from_file()

    print("预热 JIT...")
    _dummy = np.random.randn(2, len(factor_names)).astype(np.float32)
    _ = evaluate_batch(_dummy, factor_ranks, returns, GROUP_NUM)

    best_weights, best_fitness, selected_names = run_grid_search(factor_ranks, returns, factor_names)

    print("\n" + "=" * 60)
    print("最终结果")
    print("=" * 60)
    print(f"最优多空比 (Q5/Q1): {best_fitness:.4f}")

    print(f"\n选中因子及权重:")
    for name, w in zip(factor_names, best_weights):
        if w > 0:
            print(f"  {name:20s}: {w:.2f}")

    weight_sum = best_weights.sum()
    if weight_sum > 0:
        norm_weights = best_weights / weight_sum
        print(f"\n归一化权重:")
        for name, w in zip(factor_names, norm_weights):
            if w > 0:
                print(f"  {name:20s}: {w:.2f}")

    return {
        "selected_factors": selected_names,
        "weights": {name: float(w) for name, w in zip(factor_names, best_weights) if w > 0},
        "long_short_ratio": float(best_fitness),
    }


if __name__ == "__main__":
    main()
