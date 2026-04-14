"""
因子框架 - 基于动态股票池的缓存系统

原始字段（source='sql'）: 全标的缓存
因子（source='compute'）: 基于 pool 计算，截面处理只在 pool 内进行

API:
- compute_pool_factors(pool_name, pool_df, ...) - 计算 pool 因子（主要接口）
"""

import os
import time
from pathlib import Path
import sqlite3
from typing import Callable, Optional

import numpy as np
import pandas as pd
import dai

# ==================== 配置 ====================

RAW_TABLE = "cn_stock_prefactors"
FACTOR_DIR = Path(__file__).resolve().parent
CACHE_BASE_START = 20170101
MIN_CS_SAMPLE = 10


def _to_int(d: str) -> int:
    return int(d.replace("-", ""))


def _to_str(d: int) -> str:
    s = str(d)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _next_day(d: int) -> int:
    return int((pd.to_datetime(str(d), format="%Y%m%d") + pd.Timedelta(days=1)).strftime("%Y%m%d"))


def _date_to_int_series(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s)
    return (dt.dt.year * 10000 + dt.dt.month * 100 + dt.dt.day).astype(int)


def _int_to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s.astype(str), format="%Y%m%d")


# ==================== 截面处理 ====================

def winsorize_mad(s: pd.Series, n: float = 3) -> pd.Series:
    med = s.median()
    mad = (s - med).abs().median()
    if mad == 0:
        mad = s.std() * 0.6745
    return s.clip(med - n * mad, med + n * mad)


def neutralize(factor: np.ndarray, industry: pd.Series, log_cap: np.ndarray) -> np.ndarray:
    dummies = pd.get_dummies(industry, dtype=float).values
    X = np.column_stack([dummies, log_cap])
    beta, *_ = np.linalg.lstsq(X, factor, rcond=None)
    return factor - X @ beta


def process_cs(values: pd.Series, industry: pd.Series, mktcap: pd.Series) -> pd.Series:
    mask = values.notna() & industry.notna() & (mktcap > 0)
    if mask.sum() < MIN_CS_SAMPLE or industry[mask].nunique() < 2:
        return pd.Series(np.nan, index=values.index)
    v = winsorize_mad(values[mask])
    v = neutralize(v.values, industry[mask], np.log(mktcap[mask].values))
    mu, std = v.mean(), v.std()
    if std > 0:
        v = (v - mu) / std
    out = pd.Series(np.nan, index=values.index)
    out.loc[mask] = v
    return out


# ==================== 因子计算函数 ====================

def _inverse_factor(raw_field: str) -> Callable:
    def compute(df: pd.DataFrame) -> pd.Series:
        values = 1.0 / df[raw_field].replace(0, np.nan)
        return process_cs(values, df["sw2021_level1"], df["total_market_cap_raw"])
    return compute


def _identity_factor(raw_field: str) -> Callable:
    def compute(df: pd.DataFrame) -> pd.Series:
        return process_cs(df[raw_field], df["sw2021_level1"], df["total_market_cap_raw"])
    return compute


def _inverse_zscore_factor(raw_field: str) -> Callable:
    def compute(df: pd.DataFrame) -> pd.Series:
        values = 1.0 / df[raw_field].replace(0, np.nan)
        mask = values.notna()
        if mask.sum() < MIN_CS_SAMPLE:
            return pd.Series(np.nan, index=values.index)
        v = winsorize_mad(values[mask])
        mu, std = v.mean(), v.std()
        if std > 0:
            v = (v - mu) / std
        out = pd.Series(np.nan, index=values.index)
        out.loc[mask] = v
        return out
    return compute


# ==================== 缓存表 ====================

CACHE_TABLE: dict[str, dict] = {
    # 公共依赖
    'total_market_cap_raw': {'source': 'sql', 'field': 'total_market_cap'},
    'float_market_cap_raw': {'source': 'sql', 'field': 'float_market_cap'},
    'close_raw': {'source': 'sql', 'field': 'close'},
    'sw2021_level1': {'source': 'sql', 'field': 'sw2021_level1', 'is_text': True},
    # 原始字段
    'pe_ttm_raw': {'source': 'sql', 'field': 'pe_ttm'},
    'pb_raw': {'source': 'sql', 'field': 'pb'},
    'ps_ttm_raw': {'source': 'sql', 'field': 'ps_ttm'},
    'pcf_net_ttm_raw': {'source': 'sql', 'field': 'pcf_net_ttm'},
    'roe_avg_ttm_raw': {'source': 'sql', 'field': 'roe_avg_ttm'},
    'roa_avg_ttm_raw': {'source': 'sql', 'field': 'roa_avg_ttm'},
    'dividend_yield_ratio_raw': {'source': 'sql', 'field': 'dividend_yield_ratio'},
    # 因子
    'pe_ttm': {'source': 'compute', 'depends': ['pe_ttm_raw', 'total_market_cap_raw', 'sw2021_level1'], 'compute': _inverse_factor('pe_ttm_raw')},
    'pb': {'source': 'compute', 'depends': ['pb_raw', 'total_market_cap_raw', 'sw2021_level1'], 'compute': _inverse_factor('pb_raw')},
    'ps_ttm': {'source': 'compute', 'depends': ['ps_ttm_raw', 'total_market_cap_raw', 'sw2021_level1'], 'compute': _inverse_factor('ps_ttm_raw')},
    'pcf_ttm': {'source': 'compute', 'depends': ['pcf_net_ttm_raw', 'total_market_cap_raw', 'sw2021_level1'], 'compute': _inverse_factor('pcf_net_ttm_raw')},
    'roe_ttm': {'source': 'compute', 'depends': ['roe_avg_ttm_raw', 'total_market_cap_raw', 'sw2021_level1'], 'compute': _identity_factor('roe_avg_ttm_raw')},
    'roa_ttm': {'source': 'compute', 'depends': ['roa_avg_ttm_raw', 'total_market_cap_raw', 'sw2021_level1'], 'compute': _identity_factor('roa_avg_ttm_raw')},
    'dividend_yield': {'source': 'compute', 'depends': ['dividend_yield_ratio_raw', 'total_market_cap_raw', 'sw2021_level1'], 'compute': _identity_factor('dividend_yield_ratio_raw')},
    'total_market_cap': {'source': 'compute', 'depends': ['total_market_cap_raw'], 'compute': _inverse_zscore_factor('total_market_cap_raw')},
    'float_market_cap': {'source': 'compute', 'depends': ['float_market_cap_raw'], 'compute': _inverse_zscore_factor('float_market_cap_raw')},
    'close': {'source': 'compute', 'depends': ['close_raw'], 'compute': _inverse_zscore_factor('close_raw')},
}

FACTOR_NAMES = [k for k, v in CACHE_TABLE.items() if v['source'] == 'compute']


# ==================== 缓存层 ====================

def _cache_path(name: str) -> Path:
    """原始字段缓存路径: factor/{name}.sqlite"""
    return FACTOR_DIR / f"{name}.sqlite"


def _pool_factor_cache_path(pool_name: str, factor_name: str) -> Path:
    """pool 因子缓存路径: factor/{pool_name}/{factor_name}.sqlite"""
    return FACTOR_DIR / pool_name / f"{factor_name}.sqlite"


def _create_schema(conn: sqlite3.Connection, end: int, is_text: bool = False):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    val_type = "TEXT" if is_text else "REAL"
    conn.execute(
        f"CREATE TABLE data (date_int INTEGER NOT NULL, instrument TEXT NOT NULL, value {val_type}, PRIMARY KEY (date_int, instrument))")
    conn.execute("CREATE INDEX idx_date ON data(date_int)")
    conn.execute(
        "CREATE TABLE meta (range_start INTEGER NOT NULL, range_end INTEGER NOT NULL)")
    conn.execute("INSERT INTO meta VALUES (?, ?)", (CACHE_BASE_START, end))


def _create_pool_factor_schema(conn: sqlite3.Connection, pool_name: str, end: int):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        "CREATE TABLE data (date_int INTEGER NOT NULL, instrument TEXT NOT NULL, value REAL, PRIMARY KEY (date_int, instrument))")
    conn.execute("CREATE INDEX idx_date ON data(date_int)")
    conn.execute(
        "CREATE TABLE meta (pool_name TEXT NOT NULL, range_start INTEGER NOT NULL, range_end INTEGER NOT NULL)")
    conn.execute("INSERT INTO meta VALUES (?, ?, ?)", (pool_name, CACHE_BASE_START, end))


def _query_sql(field: str, start: int, end: int) -> pd.DataFrame:
    sql = f"SELECT date, instrument, {field} AS value FROM {RAW_TABLE} ORDER BY date, instrument"
    df = dai.query(sql, filters={"date": [_to_str(start), _to_str(end)]}).df()
    assert len(df) > 0, f"no data for {field} in [{start}, {end}]"
    df["date_int"] = _date_to_int_series(df["date"])
    return df[["date_int", "instrument", "value"]]


def _compute_pool_factor_data(
    pool_name: str, factor_name: str, start: int, end: int, pool_by_date: dict[int, set[str]]
) -> pd.DataFrame:
    """
    计算 pool 因子数据，截面处理只在 pool 内进行
    pool_by_date: {date_int: set(instruments)}
    """
    spec = CACHE_TABLE[factor_name]
    assert spec['source'] == 'compute', f"{factor_name} 不是计算型因子"
    deps, compute = spec['depends'], spec['compute']

    dep_data = {dep: read_cache(dep, _to_str(start), _to_str(end)) for dep in deps}
    base_df = dep_data[deps[0]][["date", "instrument"]].copy()
    for dep in deps:
        # 依赖数据必须按 (date, instrument) 严格对齐，避免静默错位
        aligned = dep_data[dep][["date", "instrument"]]
        assert aligned.equals(base_df[["date", "instrument"]]), f"{factor_name} 依赖 {dep} 对齐失败"
        base_df[dep] = dep_data[dep]["value"]
    base_df["date_int"] = _date_to_int_series(base_df["date"])

    date_ints, instruments, values = [], [], []
    day_groups = {date_int: g for date_int, g in base_df.groupby("date_int", sort=False)}
    dates_to_compute = sorted(set(day_groups) & set(pool_by_date))
    total = len(dates_to_compute)

    for i, date_int in enumerate(dates_to_compute):
        pool_insts = pool_by_date[date_int]
        day_df = day_groups[date_int]
        day_df = day_df[day_df["instrument"].isin(pool_insts)]
        if day_df.empty:
            continue
        v = compute(day_df)
        date_ints.extend([date_int] * len(day_df))
        instruments.extend(day_df["instrument"].values)
        values.extend(v.values)
        print(f"\r  [{pool_name}/{factor_name}] 计算 {i+1}/{total} ({(i+1)*100//total}%)",
              end="", flush=True)

    return pd.DataFrame({"date_int": date_ints, "instrument": instruments, "value": values})


def _insert_data(conn: sqlite3.Connection, df: pd.DataFrame):
    if not df.empty:
        df[["date_int", "instrument", "value"]].to_sql(
            "data", conn, if_exists="append", index=False, method="multi", chunksize=10000)


def ensure_cache(name: str, end_date: str) -> Path:
    """确保原始字段缓存（仅 source='sql' 类型）"""
    assert name in CACHE_TABLE, f"未知缓存项: {name}"
    spec = CACHE_TABLE[name]
    assert spec['source'] == 'sql', f"{name} 不是 sql 类型，因子请用 ensure_pool_factors"
    req_end = _to_int(end_date)
    path = _cache_path(name)
    FACTOR_DIR.mkdir(parents=True, exist_ok=True)

    if not path.exists():
        t0 = time.time()
        print(f"  [{name}] 新建 {CACHE_BASE_START}~{req_end} ", end="", flush=True)
        tmp = Path(str(path) + ".tmp")
        tmp.unlink(missing_ok=True)
        conn = sqlite3.connect(tmp)
        _create_schema(conn, req_end, spec.get('is_text', False))
        conn.commit()
        print("查询中...", end="", flush=True)
        df = _query_sql(spec['field'], CACHE_BASE_START, req_end)
        print(f" 写入 {len(df)} 行...", end="", flush=True)
        _insert_data(conn, df)
        conn.close()
        os.replace(tmp, path)
        print(f" [{time.time() - t0:.1f}s]")
        return path

    conn = sqlite3.connect(path)
    row = conn.execute("SELECT range_start, range_end FROM meta").fetchone()
    assert row and row[0] == CACHE_BASE_START, "meta invalid"
    range_end = row[1]

    if req_end <= range_end:
        conn.close()
        print(f"  [{name}] 已缓存")
        return path

    t0 = time.time()
    print(f"  [{name}] 扩展 {_next_day(range_end)}~{req_end} ", end="", flush=True)
    conn.commit()
    print("查询中...", end="", flush=True)
    df = _query_sql(spec['field'], _next_day(range_end), req_end)
    print(f" 写入 {len(df)} 行...", end="", flush=True)
    _insert_data(conn, df)
    conn.execute("UPDATE meta SET range_end = ?", (req_end,))
    conn.commit()
    conn.close()
    print(f" [{time.time() - t0:.1f}s]")
    return path


def read_cache(name: str, start_date: str, end_date: str, instruments: Optional[list] = None) -> pd.DataFrame:
    path = _cache_path(name)
    assert path.exists(), f"缓存不存在: {name}"
    start, end = _to_int(start_date), _to_int(end_date)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    if instruments:
        conn.execute("CREATE TEMP TABLE tmp_inst(instrument TEXT PRIMARY KEY)")
        conn.executemany("INSERT INTO tmp_inst VALUES (?)",
                         [(i,) for i in set(instruments)])
        sql = "SELECT d.date_int, d.instrument, d.value FROM data d INNER JOIN tmp_inst t ON d.instrument = t.instrument WHERE d.date_int BETWEEN ? AND ? ORDER BY d.date_int, d.instrument"
    else:
        sql = "SELECT date_int, instrument, value FROM data WHERE date_int BETWEEN ? AND ? ORDER BY date_int, instrument"

    df = pd.read_sql_query(sql, conn, params=[start, end])
    conn.close()
    df["date"] = _int_to_date_series(df["date_int"])
    return df.drop(columns=["date_int"])


# ==================== Pool 因子缓存 ====================

def _pool_df_to_dict(pool_df: pd.DataFrame) -> dict[int, set[str]]:
    """将 pool_df 转换为 {date_int: set(instruments)} 格式"""
    pool_df = pool_df.copy()
    pool_df["date_int"] = _date_to_int_series(pool_df["date"])
    return pool_df.groupby("date_int")["instrument"].apply(set).to_dict()


def ensure_pool_factors(
    pool_name: str,
    end_date: str,
    factor_names: list[str],
    pool_df: Optional[pd.DataFrame] = None,
) -> None:
    """
    确保 pool 的因子缓存到 end_date
    - pool_df: (date, instrument) DataFrame，扩展缓存时必须提供
    """
    req_end = _to_int(end_date)
    pool_dir = FACTOR_DIR / pool_name
    pool_dir.mkdir(parents=True, exist_ok=True)

    pool_by_date = _pool_df_to_dict(pool_df) if pool_df is not None else None

    for factor_name in factor_names:
        assert factor_name in FACTOR_NAMES, f"未知因子: {factor_name}"
        spec = CACHE_TABLE[factor_name]
        for dep in spec['depends']:
            ensure_cache(dep, end_date)

        path = _pool_factor_cache_path(pool_name, factor_name)

        if not path.exists():
            assert pool_by_date is not None, f"新建缓存 {pool_name}/{factor_name} 必须提供 pool_df"
            pool_min_date, pool_max_date = min(pool_by_date.keys()), max(pool_by_date.keys())
            assert pool_min_date <= CACHE_BASE_START, (
                f"pool_df 起始日期 {pool_min_date} 晚于缓存起始 {CACHE_BASE_START}，"
                f"需提供 {CACHE_BASE_START}~{req_end} 的完整 pool_df"
            )
            assert pool_max_date >= req_end, (
                f"pool_df 结束日期 {pool_max_date} 早于请求结束 {req_end}，"
                f"需提供 {CACHE_BASE_START}~{req_end} 的完整 pool_df"
            )
            t0 = time.time()
            print(f"  [{pool_name}/{factor_name}] 新建 {CACHE_BASE_START}~{req_end} ", end="", flush=True)
            tmp = Path(str(path) + ".tmp")
            tmp.unlink(missing_ok=True)
            conn = sqlite3.connect(tmp)
            _create_pool_factor_schema(conn, pool_name, req_end)
            conn.commit()

            df = _compute_pool_factor_data(pool_name, factor_name, CACHE_BASE_START, req_end, pool_by_date)
            print(f" 写入 {len(df)} 行...", end="", flush=True)
            _insert_data(conn, df)
            conn.close()
            os.replace(tmp, path)
            print(f" [{time.time() - t0:.1f}s]")
            continue

        conn = sqlite3.connect(path)
        row = conn.execute("SELECT pool_name, range_start, range_end FROM meta").fetchone()
        assert row and row[0] == pool_name and row[1] == CACHE_BASE_START, f"meta invalid for {pool_name}/{factor_name}"
        range_end = row[2]

        if req_end <= range_end:
            conn.close()
            print(f"  [{pool_name}/{factor_name}] 已缓存")
            continue

        assert pool_by_date is not None, f"扩展缓存 {pool_name}/{factor_name} 必须提供 pool_df"
        extend_start = _next_day(range_end)
        pool_min_date, pool_max_date = min(pool_by_date.keys()), max(pool_by_date.keys())
        assert pool_min_date <= extend_start, (
            f"pool_df 起始日期 {pool_min_date} 晚于扩展起始 {extend_start}，"
            f"需提供 {extend_start}~{req_end} 的完整 pool_df"
        )
        assert pool_max_date >= req_end, (
            f"pool_df 结束日期 {pool_max_date} 早于请求结束 {req_end}，"
            f"需提供 {extend_start}~{req_end} 的完整 pool_df"
        )
        t0 = time.time()
        print(f"  [{pool_name}/{factor_name}] 扩展 {extend_start}~{req_end} ", end="", flush=True)
        conn.commit()

        df = _compute_pool_factor_data(pool_name, factor_name, extend_start, req_end, pool_by_date)
        print(f" 写入 {len(df)} 行...", end="", flush=True)
        _insert_data(conn, df)
        conn.execute("UPDATE meta SET range_end = ?", (req_end,))
        conn.commit()
        conn.close()
        print(f" [{time.time() - t0:.1f}s]")


def read_pool_factors(
    pool_name: str,
    start_date: str,
    end_date: str,
    factor_names: list[str],
) -> pd.DataFrame:
    """读取已缓存的 pool 因子数据"""
    start, end = _to_int(start_date), _to_int(end_date)
    series_list = []

    for factor_name in factor_names:
        path = _pool_factor_cache_path(pool_name, factor_name)
        assert path.exists(), f"缓存不存在: {pool_name}/{factor_name}"
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        sql = "SELECT date_int, instrument, value FROM data WHERE date_int BETWEEN ? AND ? ORDER BY date_int, instrument"
        df = pd.read_sql_query(sql, conn, params=[start, end])
        conn.close()
        if df.empty:
            continue
        df["date"] = _int_to_date_series(df["date_int"])
        s = df.set_index(["date", "instrument"])["value"].rename(factor_name)
        series_list.append(s)

    if not series_list:
        return pd.DataFrame(columns=["date", "instrument"] + factor_names)

    result = pd.concat(series_list, axis=1, join="outer").reset_index()
    return result.sort_values(["date", "instrument"]).reset_index(drop=True)


# ==================== API ====================

def compute_pool_factors(
    pool_name: str,
    pool_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    factor_names: Optional[list] = None,
    factor_weights: Optional[dict] = None,
    score_col: str = "factor_score",
) -> pd.DataFrame:
    """
    计算 pool 因子（基于动态股票池，截面处理只在 pool 内进行）

    参数:
        pool_name: 股票池名称（用于缓存命名，如 'smallcap200'）
        pool_df: (date, instrument) DataFrame，动态股票池
        start_date: 开始日期
        end_date: 结束日期
        factor_names: 因子列表，默认全部
        factor_weights: 因子权重（用于计算加权得分）
        score_col: 得分列名
    """
    names = factor_names or FACTOR_NAMES
    ensure_pool_factors(pool_name, end_date, names, pool_df)
    factor_df = read_pool_factors(pool_name, start_date, end_date, names)

    if factor_weights:
        cols = list(factor_weights.keys())
        valid = factor_df[["date", "instrument"] + cols].dropna()
        valid[score_col] = sum(valid[c] * w for c, w in factor_weights.items())
        factor_df = factor_df.merge(
            valid[["date", "instrument", score_col]],
            on=["date", "instrument"],
            how="left",
        )

    return factor_df
