"""
网格搜索因子挖掘

优化目标: 多空组合复利 NAV (Q{GROUP_NUM} long - Q1 short, 扣费后)
因子处理: 与 strategy.compute_pool_factors 共用 factor.rank_pool_factors
    截面 pct rank [0,1] -> 加权求和 -> 再排序分档
    (保证挖掘器搜到的权重放回 strategy.py 回测时合成分数口径一致)

准确性要点:
    - 数据源: cn_stock_prefactors
    - 涨跌停粘性持仓 (对齐 strategy.py 四条限制): price_limit_status 0=缺失/1=跌停/2=正常/3=涨停
        * status != 2 的标的"冻结": 当日持仓状态 = 昨日持仓状态, 不产生换手
            - 涨停持仓不卖 (预期次日超额收益)
            - 跌停持仓不卖 (做不到)
            - 涨停非持仓不买 (做不到)
            - 跌停非持仓不买 (预期次日超额风险)
        * 只有 status == 2 的标的可自由进出 Q5 / Q1
    - 成本: 每日对 Q5/Q1 分别计算真实换手率, 乘以 COST_ROUND_TRIP (千2)
        * 低频因子年化换手 ~10倍, 对应年化成本 ~2%, 不会过度惩罚

效率要点:
    - 单 CSR 紧凑布局 (含全部 factor/ret-valid 标的 + status 标注)
    - 每日一次 argsort, 长短侧共用 (top-down 填 Q5 / bottom-up 填 Q1)
    - 锁定股 + 正常股均 O(cnt) 单次扫描, 无反向查找表
    - 内存 C-order, 手写 dot, fastmath, prange 并行
    - 粘性持仓用 per-thread bitmask O(cnt) 维护

使用方式:
    python ga_mining.py
"""

import numpy as np
import numba
from pathlib import Path
from tqdm.auto import tqdm

# ==================== 配置 ====================

DATA_FILE = Path(__file__).parent / "ga_mining_data.npz"
SCHEMA_VERSION = 1  # v1

START_DATE = "2017-01-01"
END_DATE = "2026-04-07"
GROUP_NUM = 10  # 分档数
COST_ROUND_TRIP = 0.002  # 一次换手综合成本 (买 0.0005 + 卖 0.0015)

COARSE_STEP = 0.2  # 粗搜步长
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

# npz 仍导出 FACTOR_NAMES_TO_USE 全部列; 网格搜索只在下列因子上搜权重 (须为 FACTOR_NAMES_TO_USE 子集, 顺序即权重维度顺序)
SEARCH_FACTOR_NAMES = [
    "pb",
    "ps_ttm",
    "close",
    "float_market_cap",
    "dividend_yield",
]

assert len(SEARCH_FACTOR_NAMES) >= 1
assert len(SEARCH_FACTOR_NAMES) == len(set(SEARCH_FACTOR_NAMES))
for _n in SEARCH_FACTOR_NAMES:
    assert _n in FACTOR_NAMES_TO_USE, f"SEARCH_FACTOR_NAMES 含未知因子 {_n}, 请先加入 FACTOR_NAMES_TO_USE"


# ==================== 数据导出/加载 ====================

def _load_returns_and_limits(pool_df):
    """
    加载 pool 范围内的 T+1 收益率 和 涨跌停状态
    返回: DataFrame[date, instrument, fwd_ret, price_limit_status]
    """
    import pandas as pd
    import dai

    start = pool_df["date"].min().strftime("%Y-%m-%d")
    pool_end = pool_df["date"].max().strftime("%Y-%m-%d")
    query_end = (pool_df["date"].max() + pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    sql = """
    SELECT
        date,
        instrument,
        m_lead(daily_return, 1) AS fwd_ret,
        price_limit_status
    FROM cn_stock_prefactors
    ORDER BY instrument, date
    """
    df = dai.query(sql, filters={"date": [start, query_end]}).df()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.loc[df["date"] <= pd.to_datetime(pool_end)]

    # 校验涨跌停编码 (1=跌停, 2=非涨跌停, 3=涨停)
    valid_status = df["price_limit_status"].dropna().unique()
    assert set(valid_status.tolist()).issubset({1, 2, 3}), \
        f"unexpected price_limit_status values: {valid_status}"

    # Pool 过滤
    df = df.merge(pool_df[["date", "instrument"]], on=["date", "instrument"], how="inner")
    return df


def _build_csr(
    factor_ranks: np.ndarray,     # (D, S, F) float32, NaN for missing
    returns: np.ndarray,          # (D, S) float32, NaN for missing
    limit_status: np.ndarray,     # (D, S) int8: 0=缺失, 1=跌停, 2=正常, 3=涨停
):
    """
    构造单侧 CSR 紧凑数组: 包含全部 factor/ret-valid 标的 (不过滤涨跌停)
    status 作为 flat_status 保留, 由 kernel 按粘性持仓语义处理:
        status != 2 当日持仓冻结, 只有 status == 2 可自由进出
    """
    D, S, F = factor_ranks.shape
    factor_valid = ~np.isnan(factor_ranks).any(axis=2)   # (D, S) bool
    ret_valid = ~np.isnan(returns)                        # (D, S) bool
    mask = factor_valid & ret_valid

    counts = mask.sum(axis=1).astype(np.int32)            # (D,)
    offsets = np.zeros(D + 1, dtype=np.int32)
    np.cumsum(counts, out=offsets[1:])
    total = int(offsets[-1])

    flat_ranks = np.empty((total, F), dtype=np.float32)
    flat_rets = np.empty(total, dtype=np.float32)
    flat_insts = np.empty(total, dtype=np.int32)
    flat_status = np.empty(total, dtype=np.int8)

    for d in range(D):
        lo = offsets[d]
        hi = offsets[d + 1]
        if hi == lo:
            continue
        stock_idx = np.flatnonzero(mask[d])               # (cnt,) int64
        flat_ranks[lo:hi] = factor_ranks[d, stock_idx, :]
        flat_rets[lo:hi] = returns[d, stock_idx]
        flat_insts[lo:hi] = stock_idx.astype(np.int32)
        flat_status[lo:hi] = limit_status[d, stock_idx]
    return flat_ranks, flat_rets, flat_insts, flat_status, offsets


def export_data(output_path: Path = DATA_FILE) -> None:
    """
    从数据源加载数据, 转换为 CSR 紧凑布局, 保存为压缩文件
    """
    import pandas as pd
    from factor import compute_pool_factors, rank_pool_factors
    from filter import get_universe_pool, UNIVERSE_SIZE

    POOL_NAME = f"smallcap{UNIVERSE_SIZE}"

    print("=" * 60)
    print("导出数据")
    print("=" * 60)

    print("加载股票池...")
    pool_df = get_universe_pool(START_DATE, END_DATE, UNIVERSE_SIZE)

    print("加载因子数据 (含 cache ensure)...")
    factor_df = compute_pool_factors(
        POOL_NAME, pool_df[["date", "instrument"]],
        START_DATE, END_DATE, FACTOR_NAMES_TO_USE,
    )

    print("截面 pct rank (与 compute_pool_factors 共用 rank_pool_factors)...")
    factor_df = rank_pool_factors(factor_df, FACTOR_NAMES_TO_USE)

    print("加载收益率和涨跌停状态...")
    ret_df = _load_returns_and_limits(pool_df)

    df = factor_df.merge(ret_df, on=["date", "instrument"], how="left")

    # 构建索引
    dates = sorted(df["date"].unique())
    date_to_idx = {d: i for i, d in enumerate(dates)}
    all_instruments = sorted(df["instrument"].unique())
    inst_to_idx = {inst: i for i, inst in enumerate(all_instruments)}

    D = len(dates)
    S = len(all_instruments)
    F = len(FACTOR_NAMES_TO_USE)
    print(f"  日数: {D}, 标的数: {S}, 因子数: {F}")

    factor_ranks = np.full((D, S, F), np.nan, dtype=np.float32)
    returns = np.full((D, S), np.nan, dtype=np.float32)
    limit_status = np.zeros((D, S), dtype=np.int8)  # 0 = 缺失

    d_idx = df["date"].map(date_to_idx).values.astype(np.int32)
    i_idx = df["instrument"].map(inst_to_idx).values.astype(np.int32)
    factor_values = df[FACTOR_NAMES_TO_USE].values.astype(np.float32)
    for f in range(F):
        factor_ranks[d_idx, i_idx, f] = factor_values[:, f]
    returns[d_idx, i_idx] = df["fwd_ret"].values.astype(np.float32)
    # price_limit_status 可能有 NaN (停牌/无数据), 填 0 (= 缺失, 既不能买也不能卖)
    pls = df["price_limit_status"].fillna(0).astype(np.int8).values
    limit_status[d_idx, i_idx] = pls

    print("构造 CSR 紧凑布局...")
    flat_ranks, flat_rets, flat_insts, flat_status, flat_off = _build_csr(
        factor_ranks, returns, limit_status)
    del factor_ranks, returns, limit_status

    print(f"  有效样本: {len(flat_rets)}, 日均 {len(flat_rets) / D:.1f}")

    print(f"保存到 {output_path}...")
    np.savez_compressed(
        output_path,
        schema_version=np.int32(SCHEMA_VERSION),
        ranks=flat_ranks,
        rets=flat_rets,
        insts=flat_insts,
        status=flat_status,
        off=flat_off,
        factor_names=np.array(FACTOR_NAMES_TO_USE),
        n_stocks=np.int32(S),
    )
    file_size = output_path.stat().st_size / 1024 / 1024
    print(f"文件大小: {file_size:.2f} MB")
    print("导出完成")


def load_data_from_file(input_path: Path = DATA_FILE):
    """加载单 CSR 数据 (含 status), 返回 dict"""
    assert input_path.exists(), f"数据文件不存在: {input_path}"
    print(f"从 {input_path} 加载数据...")
    d = np.load(input_path)
    assert "schema_version" in d.files and int(d["schema_version"]) == SCHEMA_VERSION, \
        f"schema 版本不匹配 (需要 v{SCHEMA_VERSION}), 请删除 {input_path} 重新导出"
    data = {
        "ranks": np.ascontiguousarray(d["ranks"], dtype=np.float32),
        "rets": np.ascontiguousarray(d["rets"], dtype=np.float32),
        "insts": np.ascontiguousarray(d["insts"], dtype=np.int32),
        "status": np.ascontiguousarray(d["status"], dtype=np.int8),
        "off": np.ascontiguousarray(d["off"], dtype=np.int32),
        "factor_names": d["factor_names"].tolist(),
        "n_stocks": int(d["n_stocks"]),
    }
    D = len(data["off"]) - 1
    print(f"  日数: {D}, 全标的数: {data['n_stocks']}, 因子数: {len(data['factor_names'])}")
    print(f"  样本: {len(data['rets'])}, 日均 {len(data['rets']) / D:.1f}")
    return data


def select_factors(data: dict, factor_indices: list[int]) -> dict:
    """选取子集因子, 返回新 data (其他 flat arrays 共享, ranks 复制为紧凑 C-order)"""
    sel = np.asarray(factor_indices, dtype=np.int64)
    new_data = dict(data)
    new_data["ranks"] = np.ascontiguousarray(data["ranks"][:, sel], dtype=np.float32)
    new_data["factor_names"] = [data["factor_names"][i] for i in factor_indices]
    return new_data


def resolve_search_indices(factor_names: list[str]) -> list[int]:
    """SEARCH_FACTOR_NAMES -> 在已加载 npz 列名中的索引"""
    idx_map = {n: i for i, n in enumerate(factor_names)}
    out: list[int] = []
    for n in SEARCH_FACTOR_NAMES:
        assert n in idx_map, f"搜索因子 {n} 不在 npz 中, 请删 ga_mining_data.npz 后重跑 export_data"
        out.append(idx_map[n])
    return out


# ==================== numba kernel ====================

@numba.njit(parallel=True, cache=True, fastmath=True, boundscheck=False)
def evaluate_batch_csr(
    pop,           # (n_pop, F) float32
    ranks,         # (N, F) float32 C-order
    rets,          # (N,) float32
    insts,         # (N,) int32
    status,        # (N,) int8: 0=缺失, 1=跌停, 2=正常, 3=涨停
    off,           # (D+1,) int32
    group_num,     # int
    cost_rt,       # float: 一次换手综合成本 (buy + sell)
    n_stocks,      # int: 全标的数, 用于 bitmask
):
    """
    粘性持仓多空评估. 每日:
        universe = 全部 factor/ret-valid 标的 (cnt), gsz = cnt // group_num
        Locked-long  = prev_long ∩ (status != 2) 今日必须持有
        Locked-short = prev_short ∩ (status != 2) 今日必须持有
        Free-long 填满 gsz: 在 status==2 里按得分从高到低补齐
        Free-short 填满 gsz: 在 status==2 里按得分从低到高补齐
        turnover_long = |new_buys| / today_long_cnt  (new_buys 只可能来自 status==2, 合法可交易)
        turnover_short 同理 (new_shorts 也只可能来自 status==2)
        daily = ret_long - ret_short - (turnover_long + turnover_short) * cost_rt
        nav *= 1 + daily
    """
    n_pop = pop.shape[0]
    F = pop.shape[1]
    D = off.shape[0] - 1
    fitness = np.empty(n_pop, dtype=np.float64)

    max_cnt = 0
    for d in range(D):
        c = off[d + 1] - off[d]
        if c > max_cnt:
            max_cnt = c
    if max_cnt < 1:
        max_cnt = 1

    for p in numba.prange(n_pop):
        w = pop[p]
        scores = np.empty(max_cnt, dtype=np.float32)
        prev_long_mask = np.zeros(n_stocks, dtype=np.uint8)
        prev_short_mask = np.zeros(n_stocks, dtype=np.uint8)
        prev_long_ids = np.empty(max_cnt, dtype=np.int32)
        prev_short_ids = np.empty(max_cnt, dtype=np.int32)
        today_long_local = np.empty(max_cnt, dtype=np.int32)
        today_short_local = np.empty(max_cnt, dtype=np.int32)
        prev_long_cnt = 0
        prev_short_cnt = 0

        nav_ls = 1.0

        for d in range(D):
            lo = off[d]
            cnt = off[d + 1] - lo
            if cnt == 0:
                continue
            gsz = cnt // group_num
            if gsz < 1:
                continue

            # 合并扫描: 计算分数 + 识别 locked (两侧)
            today_long_cnt = 0
            today_short_cnt = 0
            for i in range(cnt):
                s = 0.0
                for f in range(F):
                    s += ranks[lo + i, f] * w[f]
                scores[i] = s
                st = status[lo + i]
                if st != 2:
                    gid = insts[lo + i]
                    if prev_long_mask[gid] != 0:
                        today_long_local[today_long_cnt] = i
                        today_long_cnt += 1
                    if prev_short_mask[gid] != 0:
                        today_short_local[today_short_cnt] = i
                        today_short_cnt += 1

            order = np.argsort(scores[:cnt])  # 升序: order[0]=min, order[cnt-1]=max

            # ---------- LONG: 从高分补齐到 gsz ----------
            j = cnt - 1
            while j >= 0 and today_long_cnt < gsz:
                local = order[j]
                if status[lo + local] == 2:
                    today_long_local[today_long_cnt] = local
                    today_long_cnt += 1
                j -= 1

            if today_long_cnt > 0:
                ret_sum = 0.0
                new_buys = 0
                for k in range(today_long_cnt):
                    local = today_long_local[k]
                    ret_sum += rets[lo + local]
                    if prev_long_mask[insts[lo + local]] == 0:
                        new_buys += 1
                ret_long = ret_sum / today_long_cnt
                turnover_long = new_buys / today_long_cnt
                long_active = True
            else:
                ret_long = 0.0
                turnover_long = 0.0
                long_active = False

            # ---------- SHORT: 从低分补齐到 gsz ----------
            j = 0
            while j < cnt and today_short_cnt < gsz:
                local = order[j]
                if status[lo + local] == 2:
                    today_short_local[today_short_cnt] = local
                    today_short_cnt += 1
                j += 1

            if today_short_cnt > 0:
                ret_sum = 0.0
                new_shorts = 0
                for k in range(today_short_cnt):
                    local = today_short_local[k]
                    ret_sum += rets[lo + local]
                    if prev_short_mask[insts[lo + local]] == 0:
                        new_shorts += 1
                ret_short = ret_sum / today_short_cnt
                turnover_short = new_shorts / today_short_cnt
                short_active = True
            else:
                ret_short = 0.0
                turnover_short = 0.0
                short_active = False

            # 更新 prev_long (两套 ids 都写 gid, 便于下一日 O(1) 查 mask)
            if long_active:
                for k in range(prev_long_cnt):
                    prev_long_mask[prev_long_ids[k]] = 0
                for k in range(today_long_cnt):
                    gid = insts[lo + today_long_local[k]]
                    prev_long_mask[gid] = 1
                    prev_long_ids[k] = gid
                prev_long_cnt = today_long_cnt

            if short_active:
                for k in range(prev_short_cnt):
                    prev_short_mask[prev_short_ids[k]] = 0
                for k in range(today_short_cnt):
                    gid = insts[lo + today_short_local[k]]
                    prev_short_mask[gid] = 1
                    prev_short_ids[k] = gid
                prev_short_cnt = today_short_cnt

            if long_active and short_active:
                daily = ret_long - ret_short - (turnover_long + turnover_short) * cost_rt
                nav_ls *= (1.0 + daily)
            elif long_active:
                daily = ret_long - turnover_long * cost_rt
                nav_ls *= (1.0 + daily)
            elif short_active:
                daily = -ret_short - turnover_short * cost_rt
                nav_ls *= (1.0 + daily)

        fitness[p] = nav_ls

    return fitness


def evaluate_single(w: np.ndarray, data: dict) -> float:
    """评估单个权重向量"""
    pop = w.reshape(1, -1).astype(np.float32)
    out = evaluate_batch_csr(
        pop,
        data["ranks"], data["rets"], data["insts"], data["status"], data["off"],
        GROUP_NUM, COST_ROUND_TRIP, data["n_stocks"],
    )
    return float(out[0])


def evaluate_batch(pop: np.ndarray, data: dict) -> np.ndarray:
    pop = np.ascontiguousarray(pop, dtype=np.float32)
    return evaluate_batch_csr(
        pop,
        data["ranks"], data["rets"], data["insts"], data["status"], data["off"],
        GROUP_NUM, COST_ROUND_TRIP, data["n_stocks"],
    )


# ==================== 网格搜索 ====================

def generate_grid(n_factors: int, step: float) -> np.ndarray:
    from itertools import product
    values = np.arange(0, 1.0 + step / 2, step)
    grid = list(product(values, repeat=n_factors))
    return np.array(grid, dtype=np.float32)


def generate_fine_grid(center: np.ndarray, radius: float, step: float) -> np.ndarray:
    from itertools import product
    ranges = []
    for c in center:
        lo = max(0.0, c - radius)
        hi = min(1.0, c + radius)
        vals = np.arange(lo, hi + step / 2, step)
        ranges.append(vals)
    grid = list(product(*ranges))
    return np.array(grid, dtype=np.float32)


def iter_eval_slices(total_points: int):
    """
    按总点数自动拆分评估区间, 不使用固定 batch 常量。
    拆分批数 ~= sqrt(total_points), 在进度粒度与评估开销之间做平衡。
    """
    assert total_points >= 1
    n_batches = int(np.sqrt(total_points))
    if n_batches < 1:
        n_batches = 1
    batch_size = (total_points + n_batches - 1) // n_batches
    for st in range(0, total_points, batch_size):
        ed = st + batch_size
        if ed > total_points:
            ed = total_points
        yield st, ed


def run_grid_search(data: dict) -> tuple[np.ndarray, float, list[str]]:
    """
    多轮网格搜索
    返回: (最优权重(全因子维度), 最优多空NAV, 搜索因子名列表)
    """
    all_names = data["factor_names"]
    F_all = len(all_names)

    selected_indices = resolve_search_indices(all_names)
    selected_names = list(SEARCH_FACTOR_NAMES)
    n_search = len(selected_indices)
    print(f"\n搜索因子 ({n_search} 维): {selected_names}")

    sub_data = select_factors(data, selected_indices)

    print(f"\n[Step 1] 粗粒度网格搜索 (step={COARSE_STEP})...")
    coarse_grid = generate_grid(n_search, COARSE_STEP)
    print(f"搜索点数: {len(coarse_grid)}")
    coarse_fitness = np.empty(len(coarse_grid), dtype=np.float64)
    coarse_pbar = tqdm(total=len(coarse_grid), desc="粗搜总进度", unit="point")
    for st, ed in iter_eval_slices(len(coarse_grid)):
        coarse_fitness[st:ed] = evaluate_batch(coarse_grid[st:ed], sub_data)
        coarse_pbar.update(ed - st)
    coarse_pbar.close()

    top_k_idx = np.argsort(coarse_fitness)[-TOP_K_HOTSPOTS:][::-1]
    hotspots = coarse_grid[top_k_idx]
    hotspot_fitness = coarse_fitness[top_k_idx]
    print(f"Top {TOP_K_HOTSPOTS} 热点:")
    for i, (w, f) in enumerate(zip(hotspots, hotspot_fitness)):
        w_str = ", ".join(f"{v:.1f}" for v in w)
        print(f"  #{i+1}: [{w_str}] -> {f:.4f}")

    print(f"\n[Step 2] 细粒度搜索 (step={FINE_STEP}, radius={FINE_RADIUS})...")
    best_weights = hotspots[0].copy()
    best_fitness = float(hotspot_fitness[0])
    hotspot_pbar = tqdm(hotspots, desc="细搜热点总进度", unit="hotspot")
    for hotspot_idx, center in enumerate(hotspot_pbar, start=1):
        fine_grid = generate_fine_grid(center, FINE_RADIUS, FINE_STEP)
        fine_fitness = np.empty(len(fine_grid), dtype=np.float64)
        fine_pbar = tqdm(
            total=len(fine_grid),
            desc=f"热点{hotspot_idx}进度",
            unit="point",
            leave=False,
        )
        for st, ed in iter_eval_slices(len(fine_grid)):
            fine_fitness[st:ed] = evaluate_batch(fine_grid[st:ed], sub_data)
            fine_pbar.update(ed - st)
        fine_pbar.close()
        local_best_idx = int(np.argmax(fine_fitness))
        if fine_fitness[local_best_idx] > best_fitness:
            best_fitness = float(fine_fitness[local_best_idx])
            best_weights = fine_grid[local_best_idx].copy()
    hotspot_pbar.close()
    print(f"细搜后最优: {best_fitness:.4f}")

    full_weights = np.zeros(F_all, dtype=np.float32)
    for i, idx in enumerate(selected_indices):
        full_weights[idx] = best_weights[i]
    return full_weights, best_fitness, selected_names


# ==================== 主程序 ====================

def main():
    print("=" * 60)
    print("网格搜索因子挖掘")
    print(f"目标: 多空复利 NAV (Q{GROUP_NUM} long - Q1 short), cost_rt={COST_ROUND_TRIP}")
    print("=" * 60)

    if not DATA_FILE.exists():
        print(f"数据文件不存在, 开始生成...")
        export_data()
        print()

    data = load_data_from_file()

    print("预热 JIT...")
    _dummy_pop = np.zeros((2, len(data["factor_names"])), dtype=np.float32)
    _dummy_pop[0, 0] = 1.0
    _dummy_pop[1, 1] = 1.0
    _ = evaluate_batch(_dummy_pop, data)

    best_weights, best_fitness, selected_names = run_grid_search(data)

    print("\n" + "=" * 60)
    print("最终结果")
    print("=" * 60)
    print(f"最优多空 NAV: {best_fitness:.4f}")

    print(f"\n选中因子及权重:")
    for name, w in zip(data["factor_names"], best_weights):
        if w > 0:
            print(f"  {name:20s}: {w:.2f}")

    weight_sum = best_weights.sum()
    if weight_sum > 0:
        norm_weights = best_weights / weight_sum
        print(f"\n归一化权重:")
        for name, w in zip(data["factor_names"], norm_weights):
            if w > 0:
                print(f"  {name:20s}: {w:.2f}")

    return {
        "selected_factors": selected_names,
        "weights": {name: float(w) for name, w in zip(data["factor_names"], best_weights) if w > 0},
        "long_short_nav": float(best_fitness),
    }


if __name__ == "__main__":
    main()
