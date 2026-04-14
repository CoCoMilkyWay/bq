"""
遗传算法因子挖掘 - 极限优化版

优化目标: 5档多空收益 (Q5/Q1)
因子处理: 截面排序 -> [0,1] 分位数 -> 加权求和 -> 再排序分档

使用方式:
    python ga_mining.py
    
    第一次运行: 生成数据文件 ga_mining_data.npz
    后续运行: 直接加载数据文件做挖掘
"""

import numpy as np
import numba
from pathlib import Path

# ==================== 配置 ====================

DATA_FILE = Path(__file__).parent / "ga_mining_data.npz"

START_DATE = "2017-01-01"
END_DATE = "2026-04-07"
GROUP_NUM = 5  # 分档数

GA_POP_SIZE = 100
GA_GENERATIONS = 100
GA_MUTATION_RATE = 0.1
GA_MUTATION_SIGMA = 0.3
GA_CROSSOVER_RATE = 0.7
GA_TOURNAMENT_SIZE = 3

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


# ==================== 遗传算法 ====================

def init_population(n_pop: int, n_factors: int) -> np.ndarray:
    return np.random.uniform(-1, 1, (n_pop, n_factors)).astype(np.float32)


def tournament_select(pop: np.ndarray, fitness: np.ndarray, n_select: int, tournament_size: int) -> np.ndarray:
    n_pop = pop.shape[0]
    selected_idx = np.empty(n_select, dtype=np.int32)
    for i in range(n_select):
        candidates = np.random.choice(n_pop, tournament_size, replace=False)
        best = candidates[np.argmax(fitness[candidates])]
        selected_idx[i] = best
    return pop[selected_idx].copy()


def crossover(parents: np.ndarray, crossover_rate: float) -> np.ndarray:
    n_pop = parents.shape[0]
    n_factors = parents.shape[1]
    children = parents.copy()
    for i in range(0, n_pop - 1, 2):
        if np.random.rand() < crossover_rate:
            mask = np.random.rand(n_factors) < 0.5
            temp = children[i, mask].copy()
            children[i, mask] = children[i + 1, mask]
            children[i + 1, mask] = temp
    return children


def mutate(pop: np.ndarray, mutation_rate: float, mutation_sigma: float) -> np.ndarray:
    n_pop, n_factors = pop.shape
    mutation_mask = np.random.rand(n_pop, n_factors) < mutation_rate
    mutations = np.random.randn(n_pop, n_factors).astype(np.float32) * mutation_sigma
    pop = pop + mutations * mutation_mask
    return np.clip(pop, -1, 1)


def run_ga(factor_ranks: np.ndarray, returns: np.ndarray, factor_names: list[str]) -> tuple[np.ndarray, float]:
    """运行遗传算法"""
    n_factors = len(factor_names)

    print(f"\n开始遗传算法优化...")
    print(f"种群大小: {GA_POP_SIZE}, 代数: {GA_GENERATIONS}")
    print(f"因子数: {n_factors}, 分档数: {GROUP_NUM}")
    print(f"优化目标: Q{GROUP_NUM}/Q1 多空比")

    print("预热 JIT 编译...")
    _dummy_pop = np.random.randn(2, n_factors).astype(np.float32)
    _ = evaluate_batch(_dummy_pop, factor_ranks, returns, GROUP_NUM)

    pop = init_population(GA_POP_SIZE, n_factors)
    fitness = evaluate_batch(pop, factor_ranks, returns, GROUP_NUM)
    best_idx = np.argmax(fitness)
    best_weights = pop[best_idx].copy()
    best_fitness = fitness[best_idx]

    print(f"初始最优多空比: {best_fitness:.4f}")

    for gen in range(GA_GENERATIONS):
        parents = tournament_select(pop, fitness, GA_POP_SIZE, GA_TOURNAMENT_SIZE)
        children = crossover(parents, GA_CROSSOVER_RATE)
        children = mutate(children, GA_MUTATION_RATE, GA_MUTATION_SIGMA)
        fitness = evaluate_batch(children, factor_ranks, returns, GROUP_NUM)

        worst_idx = np.argmin(fitness)
        children[worst_idx] = best_weights
        fitness[worst_idx] = best_fitness

        gen_best_idx = np.argmax(fitness)
        if fitness[gen_best_idx] > best_fitness:
            best_fitness = fitness[gen_best_idx]
            best_weights = children[gen_best_idx].copy()

        pop = children

        if (gen + 1) % 10 == 0:
            print(f"Gen {gen + 1:3d}: best={best_fitness:.4f}, mean={fitness.mean():.4f}")

    return best_weights, best_fitness


# ==================== 主程序 ====================

def main():
    print("=" * 60)
    print("遗传算法因子挖掘")
    print("优化目标: 5档多空收益比 (Q5/Q1)")
    print("=" * 60)

    # 自动分阶段: 数据文件不存在则生成
    if not DATA_FILE.exists():
        print(f"数据文件不存在，开始生成...")
        export_data()
        print()

    factor_ranks, returns, factor_names = load_data_from_file()

    best_weights, best_fitness = run_ga(factor_ranks, returns, factor_names)

    print("\n" + "=" * 60)
    print("优化结果")
    print("=" * 60)
    print(f"最优多空比 (Q5/Q1): {best_fitness:.4f}")

    print(f"\n最优因子权重:")
    for name, w in zip(factor_names, best_weights):
        print(f"  {name:20s}: {w:+.4f}")

    norm_weights = best_weights / np.abs(best_weights).sum()
    print(f"\n归一化权重 (|w|之和=1):")
    for name, w in zip(factor_names, norm_weights):
        print(f"  {name:20s}: {w:+.4f}")

    result = {
        "weights": {name: float(w) for name, w in zip(factor_names, best_weights)},
        "norm_weights": {name: float(w) for name, w in zip(factor_names, norm_weights)},
        "long_short_ratio": float(best_fitness),
    }
    print(f"\n结果字典: {result}")

    return result


if __name__ == "__main__":
    main()
