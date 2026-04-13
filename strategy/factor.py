"""
因子框架

设计要点:
- 统一缓存层: 原始字段和因子共用同一套缓存机制
- 每个缓存项定义 source='sql' 或 source='compute'
- sql 源直接从数据库拉取，compute 源依赖其他缓存项计算

入口: build_pool_factors(...)
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
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
CACHE_BASE_START = 20250101
CACHE_MMAP_SIZE = 268435456  # 256MB
MIN_CS_SAMPLE = 10


def _to_int(d: str) -> int:
    return int(d.replace("-", ""))


def _to_str(d: int) -> str:
    s = str(d)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _next_day(d: int) -> int:
    return int((pd.Timestamp(str(d)) + pd.Timedelta(days=1)).strftime("%Y%m%d"))


# ==================== 工具函数 ====================

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

def _make_inverse_neutralized_factor(raw_field: str, direction: int = 1) -> Callable:
    def compute(df: pd.DataFrame) -> pd.Series:
        raw = df[raw_field]
        mktcap = df["total_market_cap"]
        industry = df["sw2021_level1"]
        values = 1.0 / raw.replace(0, np.nan)
        return process_cs(values, industry, mktcap) * direction
    return compute


def _make_identity_neutralized_factor(raw_field: str, direction: int = 1) -> Callable:
    def compute(df: pd.DataFrame) -> pd.Series:
        raw = df[raw_field]
        mktcap = df["total_market_cap"]
        industry = df["sw2021_level1"]
        return process_cs(raw, industry, mktcap) * direction
    return compute


# ==================== 缓存表 ====================
# source='sql': 从数据库拉取原始字段
# source='compute': 依赖其他缓存项，通过计算函数生成

CACHE_TABLE: dict[str, dict] = {
    # 公共依赖 (原始字段)
    'total_market_cap': {'source': 'sql', 'field': 'total_market_cap'},
    'sw2021_level1': {'source': 'sql', 'field': 'sw2021_level1', 'is_text': True},
    
    # 原始估值字段
    'pe_ttm_raw': {'source': 'sql', 'field': 'pe_ttm'},
    'pb_raw': {'source': 'sql', 'field': 'pb'},
    'ps_ttm_raw': {'source': 'sql', 'field': 'ps_ttm'},
    'pcf_net_ttm_raw': {'source': 'sql', 'field': 'pcf_net_ttm'},
    'roe_avg_ttm_raw': {'source': 'sql', 'field': 'roe_avg_ttm'},
    'roa_avg_ttm_raw': {'source': 'sql', 'field': 'roa_avg_ttm'},
    'dividend_yield_ratio_raw': {'source': 'sql', 'field': 'dividend_yield_ratio'},
    
    # 因子 (处理后)
    'pe_ttm': {
        'source': 'compute',
        'depends': ['pe_ttm_raw', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_inverse_neutralized_factor('pe_ttm_raw', 1),
    },
    'pb': {
        'source': 'compute',
        'depends': ['pb_raw', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_inverse_neutralized_factor('pb_raw', 1),
    },
    'ps_ttm': {
        'source': 'compute',
        'depends': ['ps_ttm_raw', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_inverse_neutralized_factor('ps_ttm_raw', 1),
    },
    'pcf_ttm': {
        'source': 'compute',
        'depends': ['pcf_net_ttm_raw', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_inverse_neutralized_factor('pcf_net_ttm_raw', 1),
    },
    'roe_ttm': {
        'source': 'compute',
        'depends': ['roe_avg_ttm_raw', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_identity_neutralized_factor('roe_avg_ttm_raw', 1),
    },
    'roa_ttm': {
        'source': 'compute',
        'depends': ['roa_avg_ttm_raw', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_identity_neutralized_factor('roa_avg_ttm_raw', 1),
    },
    'dividend_yield': {
        'source': 'compute',
        'depends': ['dividend_yield_ratio_raw', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_identity_neutralized_factor('dividend_yield_ratio_raw', 1),
    },
}

FACTOR_NAMES = [k for k, v in CACHE_TABLE.items() if v['source'] == 'compute']
FACTORS = {k: v for k, v in CACHE_TABLE.items() if v['source'] == 'compute'}  # 兼容旧接口


# ==================== 统一缓存层 ====================

def _cache_path(name: str) -> Path:
    assert name in CACHE_TABLE, f"未知缓存项: {name}"
    return CACHE_DIR / f"{name}.sqlite"


def _create_cache_schema(conn: sqlite3.Connection, end: int, is_text: bool = False):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    val_type = "TEXT" if is_text else "REAL"
    conn.execute(f"""
        CREATE TABLE data (
            date_int INTEGER NOT NULL,
            instrument TEXT NOT NULL,
            value {val_type},
            PRIMARY KEY (date_int, instrument)
        )
    """)
    conn.execute("CREATE INDEX idx_date ON data(date_int)")
    conn.execute("""
        CREATE TABLE meta (
            range_start INTEGER NOT NULL,
            range_end INTEGER NOT NULL
        )
    """)
    conn.execute("INSERT INTO meta VALUES (?, ?)", (CACHE_BASE_START, end))


def _query_sql_data(field: str, start: int, end: int) -> pd.DataFrame:
    sql = f"SELECT date, instrument, {field} AS value FROM {RAW_TABLE} ORDER BY date, instrument"
    df = dai.query(sql, filters={"date": [_to_str(start), _to_str(end)]}).df()
    assert len(df) > 0, f"no data for {field} in [{start}, {end}]"
    df["date_int"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d").astype(int)
    return df[["date_int", "instrument", "value"]]


def _compute_factor_data(name: str, start: int, end: int) -> pd.DataFrame:
    spec = CACHE_TABLE[name]
    depends = spec['depends']
    compute = spec['compute']
    
    dep_data = {dep: read_cache(dep, _to_str(start), _to_str(end)) for dep in depends}
    
    base_df = dep_data[depends[0]][["date", "instrument"]].copy()
    for dep in depends:
        base_df[dep] = dep_data[dep]["value"].values
    base_df["date_int"] = pd.to_datetime(base_df["date"]).dt.strftime("%Y%m%d").astype(int)
    
    grouped = base_df.groupby("date_int", sort=True)
    total = len(grouped)
    
    date_ints, instruments, values = [], [], []
    for i, (date_int, g) in enumerate(grouped):
        v = compute(g)
        date_ints.extend([date_int] * len(g))
        instruments.extend(g["instrument"].values)
        values.extend(v.values)
        print(f"\r  [{name}] 计算 {i+1}/{total} ({(i+1)*100//total}%)", end="", flush=True)
    
    return pd.DataFrame({"date_int": date_ints, "instrument": instruments, "value": values})


def _insert_cache_data(conn: sqlite3.Connection, df: pd.DataFrame):
    if df.empty:
        return
    df[["date_int", "instrument", "value"]].to_sql(
        "data", conn, if_exists="append", index=False, method="multi", chunksize=10000
    )


def ensure_cache(name: str, end_date: str) -> Path:
    assert name in CACHE_TABLE, f"未知缓存项: {name}"
    spec = CACHE_TABLE[name]
    req_end = _to_int(end_date)
    path = _cache_path(name)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    if spec['source'] == 'compute':
        for dep in spec['depends']:
            ensure_cache(dep, end_date)

    if not path.exists():
        t0 = time.time()
        is_text = spec.get('is_text', False)
        print(f"  [{name}] 新建 {CACHE_BASE_START}~{req_end} ", end="", flush=True)
        
        tmp = Path(str(path) + ".tmp")
        tmp.unlink(missing_ok=True)
        conn = sqlite3.connect(tmp)
        _create_cache_schema(conn, req_end, is_text)
        conn.commit()
        
        if spec['source'] == 'sql':
            print("查询中...", end="", flush=True)
            df = _query_sql_data(spec['field'], CACHE_BASE_START, req_end)
            print(f" 写入 {len(df)} 行...", end="", flush=True)
        else:
            df = _compute_factor_data(name, CACHE_BASE_START, req_end)
            print(f" 写入 {len(df)} 行...", end="", flush=True)
        
        _insert_cache_data(conn, df)
        conn.close()
        os.replace(tmp, path)
        print(f" [{time.time() - t0:.1f}s]")
        return path

    conn = sqlite3.connect(path)
    row = conn.execute("SELECT range_start, range_end FROM meta").fetchone()
    assert row, "meta missing"
    range_start, range_end = row
    assert range_start == CACHE_BASE_START

    if req_end <= range_end:
        conn.close()
        print(f"  [{name}] 已缓存")
        return path

    t0 = time.time()
    print(f"  [{name}] 扩展 {_next_day(range_end)}~{req_end} ", end="", flush=True)
    conn.commit()
    
    if spec['source'] == 'sql':
        print("查询中...", end="", flush=True)
        df = _query_sql_data(spec['field'], _next_day(range_end), req_end)
        print(f" 写入 {len(df)} 行...", end="", flush=True)
    else:
        df = _compute_factor_data(name, _next_day(range_end), req_end)
        print(f" 写入 {len(df)} 行...", end="", flush=True)
    
    _insert_cache_data(conn, df)
    conn.execute("UPDATE meta SET range_end = ?", (req_end,))
    conn.commit()
    conn.close()
    print(f" [{time.time() - t0:.1f}s]")
    return path


def read_cache(
    name: str,
    start_date: str,
    end_date: str,
    instruments: Optional[list] = None,
) -> pd.DataFrame:
    path = _cache_path(name)
    assert path.exists(), f"缓存不存在: {name}"
    start, end = _to_int(start_date), _to_int(end_date)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.execute(f"PRAGMA mmap_size={CACHE_MMAP_SIZE}")

    if instruments:
        conn.execute("CREATE TEMP TABLE tmp_inst(instrument TEXT PRIMARY KEY)")
        conn.executemany("INSERT INTO tmp_inst VALUES (?)", [(i,) for i in set(instruments)])
        sql = """
            SELECT d.date_int, d.instrument, d.value FROM data d
            INNER JOIN tmp_inst t ON d.instrument = t.instrument
            WHERE d.date_int BETWEEN ? AND ?
            ORDER BY d.date_int, d.instrument
        """
    else:
        sql = """
            SELECT date_int, instrument, value FROM data
            WHERE date_int BETWEEN ? AND ?
            ORDER BY date_int, instrument
        """
    df = pd.read_sql_query(sql, conn, params=[start, end])
    conn.close()
    df["date"] = pd.to_datetime(df["date_int"].astype(str))
    return df.drop(columns=["date_int"])


# ==================== API ====================

def compute_factors(
    start_date: str,
    end_date: str,
    factor_names: Optional[list] = None,
    instruments: Optional[list] = None,
) -> pd.DataFrame:
    names = factor_names or FACTOR_NAMES
    for n in names:
        assert n in FACTOR_NAMES, f"未知因子: {n}"

    for name in names:
        ensure_cache(name, end_date)

    frames = []
    for name in names:
        df = read_cache(name, start_date, end_date, instruments)
        df = df.rename(columns={"value": name})
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["date", "instrument"] + names)

    result = frames[0]
    for df in frames[1:]:
        result = result.merge(df, on=["date", "instrument"], how="outer")
    return result.sort_values(["date", "instrument"]).reset_index(drop=True)


def build_pool_factors(
    pool_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    factor_names: Optional[list] = None,
    factor_weights: Optional[dict] = None,
    score_col: str = "factor_score",
) -> pd.DataFrame:
    names = factor_names or FACTOR_NAMES
    for n in names:
        assert n in FACTOR_NAMES, f"未知因子: {n}"

    pool_keys = pool_df[["date", "instrument"]].drop_duplicates()
    pool_keys = pool_keys.assign(date=pd.to_datetime(pool_keys["date"]))
    instruments = pool_keys["instrument"].unique().tolist()

    factor_df = compute_factors(start_date, end_date, names, instruments)
    factor_df = factor_df.merge(pool_keys, on=["date", "instrument"], how="inner")
    factor_df = factor_df.sort_values(["date", "instrument"]).reset_index(drop=True)

    if factor_weights:
        cols = list(factor_weights.keys())
        valid = factor_df[["date", "instrument"] + cols].dropna()
        valid[score_col] = sum(valid[c] * w for c, w in factor_weights.items())
        factor_df = factor_df.merge(valid[["date", "instrument", score_col]], on=["date", "instrument"], how="left")

    return factor_df
