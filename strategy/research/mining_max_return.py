"""
Simplex lattice 因子权重挖掘

============================================================
整体流程 (按触发顺序):
============================================================

[阶段 1] 全量搜索: 对 lattice 上每个权重算 fitness Y
    触发: 1 次 batch 调用, 一次喂 P = C(n+M-1, n-1) 个权重
          n=5, M=20 -> P = 10626
    kernel: evaluate_monotonicity_csr  (parallel=True, prange over pop)
    每权重开销: 每日 (F 点积 + argsort(cnt) + G 档区间累加)
               月末 G x G 朴素排名 + Spearman + (rho+1)/2
    ≈ 全部运行时间的 99%

[阶段 2] Top-N 后评估 (三件套, 合计 ~0.1% 耗时, 基本可忽略, N=TOP_N):

    (2a) 粘性+扣费 NAV 复评   kernel: evaluate_batch_csr
         触发: 1 次 batch 调用, pop = N (top N 权重)
         目的: 给出对齐 strategy.py 实盘口径的多空/多头 NAV
         相对主搜索单点更慢 (多 bitmask 粘性 + 换手统计), 但只 N 次

    (2b) 参数平原敏感度       函数: neighbor_indices (纯 Python, 非 kernel)
         触发: top N 逐个 BFS 找 [1, NEIGHBOR_DISTANCE_MAX] 跳邻居
               一跳 = 某 k_i -1 & 另一 k_j +1 (L1=2), 单步邻居上限 n*(n-1)
               多跳总数随 N 组合增长, 默认 N=3 仍远小于 lattice 规模
         目的: 邻居 Y 均值 vs 中心 Y 的衰减, 判断是否过拟合山尖
         不重跑评估, 只从阶段 1 的 fitness 数组里查表, 开销忽略

    (2c) 最优权重年度档位表   kernel: evaluate_year_group_matrix_csr
         触发: 1 次调用, 仅对 top1 权重
         目的: 打印每年每档 (Q1..QG) 的累计收益 + 该年单调度, 便于直观检查
         单点与主 kernel 同量级, 规模差 10626x

============================================================
Fitness 定义 (阶段 1): 周期内分层单调度的跨周期均值 Y ∈ [0, 1]
============================================================
    累计周期由 FITNESS_PERIOD 控制: "year" | "month" | "week" | "day"
    每日: 按 scores = ranks @ w 升序切 G 档, 档 g 当日等权收益
          累加到 period_group[period(d), g]
    每周期: 对 G 个档的周期内累计收益做 Spearman 秩相关 rho,
            单调度 = (rho + 1) / 2 ∈ [0,1]
            有效过滤: 该周期活跃日 >= MIN_PERIOD_DAYS[FITNESS_PERIOD]
    fitness = 跨周期算术平均
    (周期越短, 惩罚短期单调性崩坏越敏感; 越长越平滑)

    注: "每日切档" 不是复杂化, 而是周期档位收益的标准日频实现
        (截面每天都变, 不能用周期级一次性分档)

============================================================
因子处理 (口径对齐)
============================================================
    与 strategy.compute_pool_factors 共用 factor.rank_pool_factors:
        截面 pct rank [0,1] -> 加权求和 -> 再排序分档
    保证挖掘器搜到的权重放回 strategy.py 回测时合成分数口径一致.

============================================================
搜索算法 (Simplex lattice / stars-and-bars)
============================================================
    观察: 每日分档只依赖 scores = ranks @ w 的相对排序 (argsort), 所以
        w 与 c*w (任意 c > 0) 产生完全相同的持仓 / 换手 / NAV.
    结论: 有意义的搜索空间是权重的 "方向", 即单位单纯形
        S = { w ∈ R^n : w_i >= 0, Σ w_i = 1 }

    Simplex lattice 在 S 上离散化: w_i = k_i / M, 其中 k_i >= 0 整数
    且 Σ k_i = M. 通过 stars-and-bars 递归枚举, 每个点是唯一归一化方向,
    无比例冗余.
        点数 = C(n + M - 1, n - 1)
        n=5, M=20 -> 10626 个点  (旧笛卡尔全网格 6^5 = 7776, 且大量冗余)

    精度: 1/M (M=20 对应步长 0.05).
    调 M 直接权衡分辨率 ↔ 评估耗时.

============================================================
准确性要点
============================================================
    - 数据源: cn_stock_prefactors
    - 搜索阶段 (fitness Y 评估): 每日 score 升序等比切分 G 档, 每档等权日收益,
        不做涨跌停粘性、不扣换手费. 这是"因子原始分层能力"指标.
    - Top-N 复评阶段 (粘性+扣费, 对齐 strategy.py 四条限制):
        price_limit_status 0=缺失/1=跌停/2=正常/3=涨停
        * status != 2 的标的"冻结": 当日持仓状态 = 昨日持仓状态, 不产生换手
            - 涨停持仓不卖 (预期次日超额收益)
            - 跌停持仓不卖 (做不到)
            - 涨停非持仓不买 (做不到)
            - 跌停非持仓不买 (预期次日超额风险)
        * 只有 status == 2 的标的可自由进出 long / short 档
        * 成本: 每日对 long/short 分别计算真实换手率, 乘以 COST_ROUND_TRIP (千2)

============================================================
效率要点
============================================================
    - 搜索点位于单位单纯形整数 lattice (k_i 整数, Σk=M), 规模随 M 温和增长
    - 整数坐标兼作 O(1) 邻居查找键, 邻居敏感度只需 dict 查表
    - 单 CSR 紧凑布局 (含全部 factor/ret-valid 标的 + status + year 标注)
    - 单调度 kernel: 每日一次 argsort + G 个区间累加, O(cnt) 扫描, 无粘性 bitmask
    - 粘性+扣费 kernel 只在 top N 上跑 (N 次调用, 可忽略)
    - 内存 C-order, 手写 dot, fastmath, prange 并行

使用方式:
    python mining.py
"""

import numpy as np
import numba
import sys
from pathlib import Path
from tqdm.auto import tqdm

# ==================== 配置 ====================

STRATEGY_DIR = Path(__file__).resolve().parents[1]
if str(STRATEGY_DIR) not in sys.path:
    sys.path.insert(0, str(STRATEGY_DIR))

DATA_FILE = Path(__file__).parent / "mining_data.npz"
SCHEMA_VERSION = 1

START_DATE = "2017-01-01"
END_DATE = "2026-04-07"
GROUP_NUM = 5  # 分档数
FITNESS_PERIOD = "month"  # 阶段 1 fitness Y 的累计周期: "year" | "month" | "week" | "day"
MIN_PERIOD_DAYS = {       # 各周期内活跃日数下限, 活跃日数 < 下限的周期不计入 fitness
    "year": 120,
    "month": 10,
    "week": 3,
    "day": 1,
}
COST_ROUND_TRIP = 0.002  # 一次换手综合成本 (买 0.0005 + 卖 0.0015)
LATTICE_M = 15  # simplex lattice 阶数: w_i = k_i / M, sum k_i = M, k_i >= 0
                # 点数 = C(n_search + M - 1, n_search - 1); 5 因子 M=20 -> 10626
TOP_N = 200  # 阶段2：按 fitness 取前 N 条做 NAV 复评、邻居表与打印 (可改)
NEIGHBOR_DISTANCE_MAX = 3  # 阶段 2b 邻居敏感度: 统计 [1, N] 跳内全部 lattice 点的 Y 均值
                           # 一跳 = 某因子 -1, 另一因子 +1 (L1=2); N=3 -> BFS 最多 3 层
STAR_LEVELS = 10  # 衰减星级分档数: 衰减升序排名分 STAR_LEVELS 档, 最低档 = STAR_LEVELS 星 (最平原)

# 不持仓月份 (1..12). 命中月份的交易日, fitness kernel 跳过 (不进 year_group, 不累加 year_days),
# NAV kernel 跳过 (持仓状态冻结, nav 不变, 无交易成本). 改此值无需重新导出数据.
SKIP_MONTHS = frozenset({1, 4, 12})

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
    "pcf_ttm",
    "close",
    "float_market_cap",
    "total_market_cap",
    "dividend_yield",
]

assert len(SEARCH_FACTOR_NAMES) >= 1
assert len(SEARCH_FACTOR_NAMES) == len(set(SEARCH_FACTOR_NAMES))
for _n in SEARCH_FACTOR_NAMES:
    assert _n in FACTOR_NAMES_TO_USE, f"SEARCH_FACTOR_NAMES 含未知因子 {_n}, 请先加入 FACTOR_NAMES_TO_USE"

for _m in SKIP_MONTHS:
    assert isinstance(_m, int) and 1 <= _m <= 12, f"SKIP_MONTHS 含非法月份 {_m}, 必须是 1..12 整数"

assert FITNESS_PERIOD in MIN_PERIOD_DAYS, \
    f"FITNESS_PERIOD={FITNESS_PERIOD} 无效, 必须是 {list(MIN_PERIOD_DAYS.keys())} 之一"


# ==================== 数据导出/加载 ====================

def _load_returns_and_limits(pool_df):
    """
    加载 pool 范围内的 T+1 收益率 和 涨跌停状态
    返回: DataFrame[date, instrument, fwd_ret, price_limit_status]
    """
    import pandas as pd
    import dai  # pyright: ignore[reportMissingImports]

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

    # 年份标号 (紧凑 0..n_years-1), 用于 kernel 内年度聚合
    year_of_day = np.array([pd.Timestamp(d).year for d in dates], dtype=np.int32)
    unique_years = np.unique(year_of_day)
    year_remap = {int(y): i for i, y in enumerate(unique_years)}
    year_idx = np.array([year_remap[int(y)] for y in year_of_day], dtype=np.int32)
    # 月份 (1..12), 保存到 npz, load 时按 SKIP_MONTHS 动态构建 active_day
    month_of_day = np.array([pd.Timestamp(d).month for d in dates], dtype=np.int8)
    # ISO (year, week) 紧凑编号, 用于 FITNESS_PERIOD="week"
    iso = pd.DatetimeIndex(dates).isocalendar()
    iso_key = iso["year"].values.astype(np.int64) * 100 + iso["week"].values.astype(np.int64)
    _, week_idx = np.unique(iso_key, return_inverse=True)
    week_idx = week_idx.astype(np.int32)
    print(f"  日数: {D}, 标的数: {S}, 因子数: {F}, 年数: {len(unique_years)} ({unique_years[0]}..{unique_years[-1]}), 周数: {int(week_idx.max()) + 1}")

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
        year_idx=year_idx,
        month_of_day=month_of_day,
        week_idx=week_idx,
        years=unique_years,
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
    off = np.ascontiguousarray(d["off"], dtype=np.int32)
    month_of_day = np.ascontiguousarray(d["month_of_day"], dtype=np.int8)
    year_idx = np.ascontiguousarray(d["year_idx"], dtype=np.int32)
    week_idx_raw = np.ascontiguousarray(d["week_idx"], dtype=np.int32)
    # active_day: 1 = 参与挖掘; 0 = 命中 SKIP_MONTHS, kernel 跳过该日
    skip_arr = np.array(sorted(SKIP_MONTHS), dtype=np.int8) if len(SKIP_MONTHS) > 0 else np.empty(0, dtype=np.int8)
    active_day = (~np.isin(month_of_day, skip_arr)).astype(np.uint8)
    D = len(off) - 1
    # 按 FITNESS_PERIOD 构造 period_idx (D,) int32 + n_periods
    # 注: SKIP_MONTHS 命中日由 active_day=0 在 kernel 内 continue 掉, period_idx 值不会被访问
    if FITNESS_PERIOD == "year":
        period_idx = year_idx.copy()
        n_periods = len(d["years"])
    elif FITNESS_PERIOD == "month":
        ym_key = year_idx.astype(np.int64) * 13 + month_of_day.astype(np.int64)
        _, period_idx = np.unique(ym_key, return_inverse=True)
        period_idx = period_idx.astype(np.int32)
        n_periods = int(period_idx.max()) + 1
    elif FITNESS_PERIOD == "week":
        period_idx = week_idx_raw
        n_periods = int(period_idx.max()) + 1
    elif FITNESS_PERIOD == "day":
        period_idx = np.arange(D, dtype=np.int32)
        n_periods = D
    else:
        assert False, f"unreachable FITNESS_PERIOD={FITNESS_PERIOD}"
    data = {
        "ranks": np.ascontiguousarray(d["ranks"], dtype=np.float32),
        "rets": np.ascontiguousarray(d["rets"], dtype=np.float32),
        "insts": np.ascontiguousarray(d["insts"], dtype=np.int32),
        "status": np.ascontiguousarray(d["status"], dtype=np.int8),
        "off": off,
        "year_idx": year_idx,
        "month_of_day": month_of_day,
        "period_idx": period_idx,
        "n_periods": n_periods,
        "min_period_days": MIN_PERIOD_DAYS[FITNESS_PERIOD],
        "active_day": active_day,
        "years": d["years"].tolist(),
        "factor_names": d["factor_names"].tolist(),
        "n_stocks": int(d["n_stocks"]),
        "max_cnt": int(max(1, np.diff(off).max())),  # kernel 内共享, 避免重复扫描
    }
    n_active = int(active_day.sum())
    skip_disp = sorted(SKIP_MONTHS) if SKIP_MONTHS else "无"
    print(f"  日数: {D}, 全标的数: {data['n_stocks']}, 因子数: {len(data['factor_names'])}, 年数: {len(data['years'])} ({data['years'][0]}..{data['years'][-1]}), 周期=\"{FITNESS_PERIOD}\" 桶数: {n_periods}")
    print(f"  样本: {len(data['rets'])}, 日均 {len(data['rets']) / D:.1f}")
    print(f"  SKIP_MONTHS={skip_disp}, 活跃日数: {n_active}/{D} ({100.0 * n_active / D:.1f}%)")
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
        assert n in idx_map, f"搜索因子 {n} 不在 npz 中, 请删 mining_data.npz 后重跑 export_data"
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
    active_day,    # (D,) uint8, 0=SKIP_MONTHS 命中日, 持仓冻结 nav 不变
    year_idx,      # (D,) int32, 0..n_years-1
    n_years,       # int
    group_num,     # int
    cost_rt,       # float: 一次换手综合成本 (buy + sell)
    n_stocks,      # int: 全标的数, 用于 bitmask
    max_cnt,       # int, 预计算的最大日样本数
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
        daily_ls   = ret_long - ret_short - (turnover_long + turnover_short) * cost_rt
        daily_long = ret_long - turnover_long * cost_rt
    NAV 按年分桶累乘 (每年起算 1.0):
        cum_nav = Π_y year_nav[y]      (等价全期累乘)
        avg_nav = mean_y year_nav[y]   (各年年末 NAV 算术平均)
    返回: fitness (n_pop, 4)
        [:,0] = 累计多空 NAV
        [:,1] = 累计多头 NAV
        [:,2] = 年均多空 NAV (逐年重置, 年末 NAV 算术平均)
        [:,3] = 年均多头 NAV
    """
    n_pop = pop.shape[0]
    F = pop.shape[1]
    D = off.shape[0] - 1
    fitness = np.empty((n_pop, 4), dtype=np.float64)

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

        year_nav_ls = np.ones(n_years, dtype=np.float64)
        year_nav_l = np.ones(n_years, dtype=np.float64)
        year_seen = np.zeros(n_years, dtype=np.uint8)

        for d in range(D):
            if active_day[d] == 0:
                # SKIP_MONTHS 命中日: 持仓冻结, nav 不变, 无交易成本 (选项 C 语义)
                continue
            lo = off[d]
            cnt = off[d + 1] - lo
            if cnt == 0:
                continue
            gsz = cnt // group_num
            if gsz < 1:
                continue
            y = year_idx[d]
            year_seen[y] = 1

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
                year_nav_ls[y] *= (1.0 + daily)
            elif long_active:
                daily = ret_long - turnover_long * cost_rt
                year_nav_ls[y] *= (1.0 + daily)
            elif short_active:
                daily = -ret_short - turnover_short * cost_rt
                year_nav_ls[y] *= (1.0 + daily)

            if long_active:
                year_nav_l[y] *= (1.0 + ret_long - turnover_long * cost_rt)

        cum_ls = 1.0
        cum_l = 1.0
        sum_ls = 0.0
        sum_l = 0.0
        n_seen = 0
        for y in range(n_years):
            if year_seen[y] != 0:
                cum_ls *= year_nav_ls[y]
                cum_l *= year_nav_l[y]
                sum_ls += year_nav_ls[y]
                sum_l += year_nav_l[y]
                n_seen += 1
        if n_seen > 0:
            avg_ls = sum_ls / n_seen
            avg_l = sum_l / n_seen
        else:
            avg_ls = 1.0
            avg_l = 1.0
        fitness[p, 0] = cum_ls
        fitness[p, 1] = cum_l
        fitness[p, 2] = avg_ls
        fitness[p, 3] = avg_l

    return fitness


def evaluate_batch(pop: np.ndarray, data: dict) -> np.ndarray:
    """
    粘性+扣费 多空/多头 NAV 评估. 返回 (n_pop, 4):
        [:,0]=累计多空, [:,1]=累计多头, [:,2]=年均多空, [:,3]=年均多头.
    """
    pop = np.ascontiguousarray(pop, dtype=np.float32)
    return evaluate_batch_csr(
        pop,
        data["ranks"], data["rets"], data["insts"], data["status"], data["off"],
        data["active_day"],
        data["year_idx"], len(data["years"]),
        GROUP_NUM, COST_ROUND_TRIP, data["n_stocks"], data["max_cnt"],
    )


@numba.njit(cache=True, fastmath=True, boundscheck=False, inline='always')
def _accum_year_group(
    w,             # (F,) float32, 单个权重
    ranks,         # (N, F) float32 C-order
    rets,          # (N,) float32
    off,           # (D+1,) int32
    year_idx,      # (D,) int32
    active_day,    # (D,) uint8, 0=SKIP_MONTHS 命中日, kernel 跳过
    G,             # int
    scores,        # (max_cnt,) float32, 调用方分配复用 buffer
    year_group,    # (n_years, G) float64, 调用方 zero out
    year_days,     # (n_years,) int32, 调用方 zero out
):
    """
    核心 primitive: 按 w 每日 score→argsort→等分 G 档→按年累加档内等权日收益.
    Spearman / NAV / year matrix 三种用法都复用这个累加结果.
    active_day[d]==0 的日子整日跳过 (不累加, 不计入 year_days).
    """
    F = w.shape[0]
    D = off.shape[0] - 1
    for d in range(D):
        if active_day[d] == 0:
            continue
        lo = off[d]
        cnt = off[d + 1] - lo
        if cnt < G:
            continue
        for i in range(cnt):
            s = 0.0
            for f in range(F):
                s += ranks[lo + i, f] * w[f]
            scores[i] = s
        order = np.argsort(scores[:cnt])  # 升序, order[0]=min (Q1), order[cnt-1]=max (QG)
        y = year_idx[d]
        for g in range(G):
            lo_g = cnt * g // G
            hi_g = cnt * (g + 1) // G
            sz = hi_g - lo_g
            if sz < 1:
                continue
            sum_ret = 0.0
            for k in range(lo_g, hi_g):
                sum_ret += rets[lo + order[k]]
            year_group[y, g] += sum_ret / sz
        year_days[y] += 1


@numba.njit(parallel=True, cache=True, fastmath=True, boundscheck=False)
def evaluate_monotonicity_csr(
    pop,           # (n_pop, F) float32
    ranks,         # (N, F) float32 C-order
    rets,          # (N,) float32
    off,           # (D+1,) int32
    period_idx,    # (D,) int32, 0..n_periods-1 (紧凑周期索引, 粒度由 FITNESS_PERIOD 决定)
    active_day,    # (D,) uint8
    n_periods,     # int
    group_num,     # int
    min_days,      # int, 周期内活跃日数下限, < 此值的周期不计入
    max_cnt,       # int, 预计算的最大日样本数
):
    """
    周期分层单调度 fitness. 调 _accum_year_group (bucket 通用 primitive) 拿到
    period_group 后, 每周期做一次无并列 Spearman: rho = 1 - 6Σd²/(G(G²-1)),
    score_p = (rho+1)/2. fitness = 有效周期 (周期活跃日 >= min_days) 均值 ∈ [0,1].
    """
    n_pop = pop.shape[0]
    G = group_num
    fitness = np.empty(n_pop, dtype=np.float64)
    spearman_denom = float(G * (G * G - 1))  # G>=2 保证 > 0

    for p in numba.prange(n_pop):
        scores = np.empty(max_cnt, dtype=np.float32)
        period_group = np.zeros((n_periods, G), dtype=np.float64)
        period_days = np.zeros(n_periods, dtype=np.int32)
        _accum_year_group(pop[p], ranks, rets, off, period_idx, active_day, G,
                          scores, period_group, period_days)

        total = 0.0
        n_valid = 0
        for m in range(n_periods):
            if period_days[m] < min_days:
                continue
            ssq = 0.0
            for g in range(G):
                r = 1
                for h in range(G):
                    if period_group[m, h] < period_group[m, g]:
                        r += 1
                    elif period_group[m, h] == period_group[m, g] and h < g:
                        r += 1
                diff = float(r - (g + 1))
                ssq += diff * diff
            rho = 1.0 - 6.0 * ssq / spearman_denom
            total += (rho + 1.0) * 0.5
            n_valid += 1

        if n_valid == 0:
            fitness[p] = 0.0
        else:
            fitness[p] = total / n_valid

    return fitness


def evaluate_monotonicity(pop: np.ndarray, data: dict) -> np.ndarray:
    """周期分层单调度 fitness ∈ [0,1]. 返回 (n_pop,)."""
    pop = np.ascontiguousarray(pop, dtype=np.float32)
    return evaluate_monotonicity_csr(
        pop,
        data["ranks"], data["rets"], data["off"],
        data["period_idx"], data["active_day"],
        data["n_periods"], GROUP_NUM, data["min_period_days"], data["max_cnt"],
    )


@numba.njit(cache=True, fastmath=True, boundscheck=False)
def evaluate_year_group_matrix_csr(
    w,             # (F,) float32, 单个权重
    ranks,         # (N, F) float32 C-order
    rets,          # (N,) float32
    off,           # (D+1,) int32
    year_idx,      # (D,) int32
    active_day,    # (D,) uint8
    n_years,       # int
    group_num,     # int
    max_cnt,       # int
):
    """单个权重 w 的年度档位收益矩阵. 直接包装 _accum_year_group."""
    G = group_num
    scores = np.empty(max_cnt, dtype=np.float32)
    year_group = np.zeros((n_years, G), dtype=np.float64)
    year_days = np.zeros(n_years, dtype=np.int32)
    _accum_year_group(w, ranks, rets, off, year_idx, active_day, G,
                      scores, year_group, year_days)
    return year_group, year_days


# ==================== Simplex lattice 搜索 ====================

def generate_simplex_lattice(n_factors: int, m: int) -> np.ndarray:
    """
    n 维单位单纯形上 M 阶 lattice 的整数坐标 k:
        k_i >= 0 整数, Σk_i = m; 对应权重 w = k / m.
    stars-and-bars 递归枚举, 点数 = C(n+m-1, n-1).
    返回: (P, n) int32. 浮点权重由调用方自行除 m 得到.
    """
    assert n_factors >= 1 and m >= 1
    points: list[tuple[int, ...]] = []
    k = [0] * n_factors

    def rec(i: int, remain: int) -> None:
        if i == n_factors - 1:
            k[i] = remain
            points.append(tuple(k))
            return
        for v in range(remain + 1):
            k[i] = v
            rec(i + 1, remain - v)

    rec(0, m)
    return np.asarray(points, dtype=np.int32)


def neighbor_indices(k_grid: np.ndarray, center_idx: int, key_to_idx: dict, max_dist: int) -> list[int]:
    """
    在 simplex lattice 上 BFS 查找与 center 距离在 [1, max_dist] 跳内的全部邻居 (不含 center).
    一跳 = 从某非零因子转移 1 单位到另一因子 (k_i-=1, k_j+=1; L1 距离=2).
    返回的索引顺序无特殊语义, 调用方只用于查 fitness 做算术平均.
    """
    assert max_dist >= 1
    center_key = tuple(int(v) for v in k_grid[center_idx])
    n = len(center_key)
    visited: set[tuple[int, ...]] = {center_key}
    frontier: list[tuple[int, ...]] = [center_key]
    out: list[int] = []
    for _hop in range(max_dist):
        next_frontier: list[tuple[int, ...]] = []
        for key in frontier:
            for i in range(n):
                if key[i] == 0:
                    continue
                for j in range(n):
                    if i == j:
                        continue
                    new_list = list(key)
                    new_list[i] -= 1
                    new_list[j] += 1
                    new_key = tuple(new_list)
                    if new_key in visited:
                        continue
                    visited.add(new_key)
                    # 一步 ±1 转移保持 Σk=M 与 k>=0, 必然在 lattice 内
                    assert new_key in key_to_idx
                    out.append(key_to_idx[new_key])
                    next_frontier.append(new_key)
        if not next_frontier:
            break
        frontier = next_frontier
    return out


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


def run_grid_search(data: dict, top_n: int | None = None) -> tuple[np.ndarray, float, list[str]]:
    """
    两阶段评估:
      1) 全量 lattice 扫 fitness Y = 粘性+扣费 多头累计 NAV (全期, 对齐 strategy.py 口径)
      2) Top-K 权重额外算:
         - 邻居平均 Y (L1=2, 参数平原 / 过拟合敏感度)
         - 粘性+扣费口径的全期多空 NAV, 年均多头/多空 NAV
         - 年度分档累计收益表 (干净等权, 用于诊断)
    返回: (最优权重(全因子维度, 已归一化), 最优 Y, 搜索因子名列表)
    """
    all_names = data["factor_names"]
    F_all = len(all_names)
    n_years = len(data["years"])
    years = data["years"]

    selected_indices = resolve_search_indices(all_names)
    selected_names = list(SEARCH_FACTOR_NAMES)
    n_search = len(selected_indices)
    print(f"\n搜索因子 ({n_search} 维): {selected_names}")

    sub_data = select_factors(data, selected_indices)

    k_grid = generate_simplex_lattice(n_search, LATTICE_M)  # (P, n) int32
    w_grid = (k_grid.astype(np.float32) / np.float32(LATTICE_M))  # (P, n) float32
    print(f"Simplex lattice M={LATTICE_M}, 点数={len(w_grid)}, 步长=1/{LATTICE_M}={1.0 / LATTICE_M:.4f}")

    fitness = np.empty(len(w_grid), dtype=np.float64)
    pbar = tqdm(total=len(w_grid), desc="多头累计 NAV 搜索", unit="point")
    for st, ed in iter_eval_slices(len(w_grid)):
        fitness[st:ed] = evaluate_batch(w_grid[st:ed], sub_data)[:, 1]
        pbar.update(ed - st)
    pbar.close()

    n_top = TOP_N if top_n is None else top_n
    assert n_top >= 1
    top_k = min(n_top, len(w_grid))
    top_idx = np.argsort(fitness)[-top_k:][::-1]

    # Top-K 粘性+扣费 NAV (累计 + 年均, 多空 + 多头)
    top_w = w_grid[top_idx]
    top_nav = evaluate_batch(top_w, sub_data)  # (top_k, 4)

    # 邻居查找表: tuple(k) -> grid_idx
    key_to_idx = {tuple(int(v) for v in k): i for i, k in enumerate(k_grid)}

    # 收集 top-k 明细, 先算衰减再映射星级 (邻居 = [1, NEIGHBOR_DISTANCE_MAX] 跳内全部点)
    nbr_counts = np.empty(top_k, dtype=np.int32)
    nbr_means = np.empty(top_k, dtype=np.float64)
    decays = np.empty(top_k, dtype=np.float64)
    for i, gi in enumerate(top_idx):
        nbrs = neighbor_indices(k_grid, gi, key_to_idx, NEIGHBOR_DISTANCE_MAX)
        nbr_counts[i] = len(nbrs)
        nbr_means[i] = float(np.mean(fitness[nbrs])) if nbrs else float("nan")
        decays[i] = float(fitness[gi]) - nbr_means[i]

    # 衰减升序 rank -> 1..STAR_LEVELS 星 (最低衰减档 = STAR_LEVELS 星 = 最平原, 最高衰减档 = 1 星 = 最山尖)
    assert STAR_LEVELS >= 1
    order = np.argsort(decays)
    d_rank = np.empty_like(order)
    d_rank[order] = np.arange(top_k)
    star_counts = np.empty(top_k, dtype=np.int32)
    for i in range(top_k):
        b = int(d_rank[i] * STAR_LEVELS / max(top_k, 1))
        if b > STAR_LEVELS - 1:
            b = STAR_LEVELS - 1
        star_counts[i] = STAR_LEVELS - b

    skip_disp = sorted(SKIP_MONTHS) if SKIP_MONTHS else "无"
    pct_per_bin = 100.0 / STAR_LEVELS
    star_w = STAR_LEVELS
    nbr_col_w = max(3, len(str(int(nbr_counts.max()))) if top_k > 0 else 3)
    print(f"\nTop {top_k} 结果 (搜索因子顺序: {selected_names}, SKIP_MONTHS={skip_disp}):")
    print("列含义:")
    print("  #        : 在 lattice 内按 Y 降序的名次")
    print(f"  Y        : 粘性+扣费 多头累计 NAV (cost_rt={COST_ROUND_TRIP}, 全期起点 1.0, 对齐 strategy.py 实盘口径)")
    print(f"  NbrY     : [1,{NEIGHBOR_DISTANCE_MAX}] 跳内全部邻居权重的 Y 均值 (扰动稳定性)")
    print("  衰减     : Y - NbrY, 越接近 0 越抗过拟合 (山尖 vs 平原)")
    print(f"  星       : 衰减在 top-N 内升序 {STAR_LEVELS} 分位 ({STAR_LEVELS}★=衰减最低 {pct_per_bin:.1f}% 最平原, 1★=最高 {pct_per_bin:.1f}% 最山尖)")
    print(f"  N        : [1,{NEIGHBOR_DISTANCE_MAX}] 跳内邻居总个数 (边界点会减少)")
    print(f"  多头年均 : 逐年重置 NAV, 各年年末 NAV 算术平均 (1.0 = 当年持平)")
    print(f"  多空累计 : 粘性+扣费 下 top-bottom 多空 NAV (起点 1.0)")
    print(f"  多空年均 : 逐年重置多空 NAV, 各年年末算术平均")
    print(f"  权重     : 搜索因子维度上的权重 (顺序同上, 已归一化 sum=1)")
    header = f"{'#':<3} {'Y':>8} {'NbrY':>8} {'衰减':>8} {'星':<{star_w}} {'N':>{nbr_col_w}} {'多头年均':>8} {'多空累计':>8} {'多空年均':>8}  权重"
    print(header)
    print("-" * len(header))
    for rank, gi in enumerate(top_idx, 1):
        i = rank - 1
        w_str = ", ".join(f"{v:.2f}" for v in w_grid[gi])
        stars = "*" * int(star_counts[i])
        nav_ls_cum = float(top_nav[i, 0])
        nav_ls_avg = float(top_nav[i, 2])
        nav_l_avg = float(top_nav[i, 3])
        print(
            f"{rank:<3} {fitness[gi]:8.3f} {nbr_means[i]:8.3f} {decays[i]:+8.3f} "
            f"{stars:<{star_w}} {int(nbr_counts[i]):{nbr_col_w}d} "
            f"{nav_l_avg:8.3f} {nav_ls_cum:8.3f} {nav_ls_avg:8.3f}  [{w_str}]"
        )

    # 最优权重的年度各档累计收益表 (干净等权, 算术累加)
    best_idx = int(top_idx[0])
    best_weights = w_grid[best_idx].copy()
    best_fitness = float(fitness[best_idx])
    yg, yd = evaluate_year_group_matrix_csr(
        best_weights,
        sub_data["ranks"], sub_data["rets"], sub_data["off"],
        sub_data["year_idx"], sub_data["active_day"],
        n_years, GROUP_NUM, sub_data["max_cnt"],
    )
    print(f"\n最优权重年度各档累计收益 (干净等权, 算术累加, 未扣费; SKIP_MONTHS={skip_disp} 不计入):")
    print(f"  Q1..Q{GROUP_NUM} = 按 score 升序切分的分位档 (Q1=最低分组, Q{GROUP_NUM}=最高分组)")
    print(f"  单元格 = 该档全年所有活跃日 (剔除 SKIP 月) 等权日收益的算术累加")
    hdr = "年份 " + " ".join(f"Q{g + 1:<6}" for g in range(GROUP_NUM)) + "  单调度"
    print(hdr)
    for y in range(n_years):
        if yd[y] < GROUP_NUM:
            continue
        row_vals = yg[y]
        # 单独再算一次该年单调度用于展示
        ranks_sorted = np.argsort(np.argsort(row_vals))  # 0..G-1 秩
        ssq = float(np.sum((ranks_sorted - np.arange(GROUP_NUM)) ** 2))
        rho = 1.0 - 6.0 * ssq / (GROUP_NUM * (GROUP_NUM ** 2 - 1))
        y_score = (rho + 1.0) * 0.5
        cells = " ".join(f"{v:+.4f}" for v in row_vals)
        print(f"{years[y]} {cells}  {y_score:.3f}")

    full_weights = np.zeros(F_all, dtype=np.float32)
    for i, idx in enumerate(selected_indices):
        full_weights[idx] = best_weights[i]
    return full_weights, best_fitness, selected_names


# ==================== 主程序 ====================

def main():
    skip_disp = sorted(SKIP_MONTHS) if SKIP_MONTHS else "无"
    print("=" * 60)
    print("Simplex lattice 因子权重搜索")
    print(f"目标: 粘性+扣费 多头累计 NAV (cost_rt={COST_ROUND_TRIP}, 累计周期 = 数据全量)")
    print(f"Top{TOP_N} 复评: 邻居 L1=2 敏感度 + 年度分档表")
    print(f"SKIP_MONTHS={skip_disp} (命中日 fitness 与 NAV 均跳过, 持仓冻结)")
    print("=" * 60)

    if not DATA_FILE.exists():
        print(f"数据文件不存在, 开始生成...")
        export_data()
        print()

    data = load_data_from_file()

    print("预热 JIT...")
    _dummy = np.zeros((1, len(data["factor_names"])), dtype=np.float32)
    _dummy[0, 0] = 1.0
    evaluate_batch(_dummy, data)
    evaluate_monotonicity(_dummy, data)

    best_weights, best_fitness, selected_names = run_grid_search(data)

    print("\n" + "=" * 60)
    print("最终结果")
    print("=" * 60)
    print(f"最优多头累计 NAV: {best_fitness:.4f}")
    print(f"\n最优权重 (sum=1):")
    for name, w in zip(data["factor_names"], best_weights):
        if w > 0:
            print(f"  {name:20s}: {w:.4f}")

    return {
        "selected_factors": selected_names,
        "weights": {name: float(w) for name, w in zip(data["factor_names"], best_weights) if w > 0},
        "mean_monotonicity": float(best_fitness),
    }


if __name__ == "__main__":
    main()
