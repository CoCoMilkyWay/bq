"""
因子框架

设计要点:
- 缓存层: 每个字段独立 sqlite，统一由 CACHE_TABLE 定义
- 因子层: 每个因子独立定义计算函数，由 FACTOR_TABLE 注册
- 工具函数: 提供 winsorize_mad / neutralize / process_cs 供因子复用

入口: build_pool_factors(...)
"""

import os
from pathlib import Path
import sqlite3
from typing import Callable, Optional

import numpy as np
import pandas as pd
import dai


# ==================== 配置 ====================

RAW_TABLE = "cn_stock_prefactors"
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
CACHE_BASE_START = 20170101
CACHE_MMAP_SIZE = 268435456  # 256MB
MIN_CS_SAMPLE = 10


# ==================== 缓存表 ====================
# name -> source_field (来自 RAW_TABLE)
# 每个缓存项生成独立的 .cache/{name}.sqlite

CACHE_TABLE: dict[str, str] = {
    # 中间数据
    'total_market_cap': 'total_market_cap',
    'sw2021_level1': 'sw2021_level1',
    # 原始估值字段
    'pe_ttm': 'pe_ttm',
    'pb': 'pb',
    'ps_ttm': 'ps_ttm',
    'pcf_net_ttm': 'pcf_net_ttm',
    'roe_avg_ttm': 'roe_avg_ttm',
    'roa_avg_ttm': 'roa_avg_ttm',
    'dividend_yield_ratio': 'dividend_yield_ratio',
}


def _to_int(d: str) -> int:
    return int(d.replace("-", ""))


def _to_str(d: int) -> str:
    s = str(d)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _next_day(d: int) -> int:
    return int((pd.Timestamp(str(d)) + pd.Timedelta(days=1)).strftime("%Y%m%d"))


# ==================== 缓存层 ====================

def cache_path(name: str) -> Path:
    assert name in CACHE_TABLE, f"未知缓存: {name}"
    return CACHE_DIR / f"{name}.sqlite"


def _create_cache_schema(conn: sqlite3.Connection, end: int, is_text: bool):
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


def _query_cache_raw(field: str, start: int, end: int) -> pd.DataFrame:
    sql = f"SELECT date, instrument, {field} AS value FROM {RAW_TABLE} ORDER BY date, instrument"
    df = dai.query(sql, filters={"date": [_to_str(start), _to_str(end)]}).df()
    assert len(df) > 0, f"no data for {field} in [{start}, {end}]"
    df["date_int"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d").astype(int)
    return df[["date_int", "instrument", "value"]].sort_values(["date_int", "instrument"]).reset_index(drop=True)


def _insert_cache_rows(conn: sqlite3.Connection, df: pd.DataFrame):
    if df.empty:
        return
    conn.executemany(
        "INSERT OR REPLACE INTO data (date_int, instrument, value) VALUES (?, ?, ?)",
        df[["date_int", "instrument", "value"]].itertuples(index=False, name=None),
    )


def ensure_cache(name: str, end_date: str) -> Path:
    assert name in CACHE_TABLE, f"未知缓存: {name}"
    field = CACHE_TABLE[name]
    is_text = name == "sw2021_level1"
    req_end = _to_int(end_date)
    path = cache_path(name)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if not path.exists():
        tmp = Path(str(path) + ".tmp")
        tmp.unlink(missing_ok=True)
        conn = sqlite3.connect(tmp)
        _create_cache_schema(conn, req_end, is_text)
        df = _query_cache_raw(field, CACHE_BASE_START, req_end)
        conn.execute("BEGIN IMMEDIATE")
        _insert_cache_rows(conn, df)
        conn.commit()
        conn.close()
        os.replace(tmp, path)
        return path

    conn = sqlite3.connect(path)
    row = conn.execute("SELECT range_start, range_end FROM meta").fetchone()
    assert row, "meta missing"
    range_start, range_end = row
    assert range_start == CACHE_BASE_START

    if req_end <= range_end:
        conn.close()
        return path

    df = _query_cache_raw(field, _next_day(range_end), req_end)
    conn.execute("BEGIN IMMEDIATE")
    _insert_cache_rows(conn, df)
    conn.execute("UPDATE meta SET range_end = ?", (req_end,))
    conn.commit()
    conn.close()
    return path


def read_cache(
    name: str,
    start_date: str,
    end_date: str,
    instruments: Optional[list] = None,
) -> pd.DataFrame:
    path = cache_path(name)
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


# ==================== 因子表 ====================

def _make_inverse_neutralized_factor(raw_field: str, direction: int = 1) -> Callable:
    def compute(data: dict[str, pd.DataFrame]) -> pd.Series:
        raw = data[raw_field]["value"]
        mktcap = data["total_market_cap"]["value"]
        industry = data["sw2021_level1"]["value"]
        values = 1.0 / raw.replace(0, np.nan)
        return process_cs(values, industry, mktcap) * direction
    return compute


def _make_identity_neutralized_factor(raw_field: str, direction: int = 1) -> Callable:
    def compute(data: dict[str, pd.DataFrame]) -> pd.Series:
        raw = data[raw_field]["value"]
        mktcap = data["total_market_cap"]["value"]
        industry = data["sw2021_level1"]["value"]
        return process_cs(raw, industry, mktcap) * direction
    return compute


FACTOR_TABLE: dict[str, dict] = {
    'pe_ttm': {
        'depends': ['pe_ttm', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_inverse_neutralized_factor('pe_ttm', 1),
    },
    'pb': {
        'depends': ['pb', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_inverse_neutralized_factor('pb', 1),
    },
    'ps_ttm': {
        'depends': ['ps_ttm', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_inverse_neutralized_factor('ps_ttm', 1),
    },
    'pcf_ttm': {
        'depends': ['pcf_net_ttm', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_inverse_neutralized_factor('pcf_net_ttm', 1),
    },
    'roe_ttm': {
        'depends': ['roe_avg_ttm', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_identity_neutralized_factor('roe_avg_ttm', 1),
    },
    'roa_ttm': {
        'depends': ['roa_avg_ttm', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_identity_neutralized_factor('roa_avg_ttm', 1),
    },
    'dividend_yield': {
        'depends': ['dividend_yield_ratio', 'total_market_cap', 'sw2021_level1'],
        'compute': _make_identity_neutralized_factor('dividend_yield_ratio', 1),
    },
}

FACTORS = FACTOR_TABLE  # 兼容旧接口


# ==================== API ====================

def _get_all_depends(names: list[str]) -> set[str]:
    deps = set()
    for n in names:
        deps.update(FACTOR_TABLE[n]["depends"])
    return deps


def _load_cache_data(
    names: list[str],
    start_date: str,
    end_date: str,
    instruments: Optional[list] = None,
) -> dict[str, pd.DataFrame]:
    deps = _get_all_depends(names)
    for dep in deps:
        ensure_cache(dep, end_date)
    return {dep: read_cache(dep, start_date, end_date, instruments) for dep in deps}


def _compute_factors_by_day(
    day_data: dict[str, pd.DataFrame],
    names: list[str],
) -> pd.DataFrame:
    first_df = next(iter(day_data.values()))
    out = first_df[["date", "instrument"]].copy()
    for name in names:
        factor_def = FACTOR_TABLE[name]
        out[name] = factor_def["compute"](day_data)
    return out


def compute_factors(
    start_date: str,
    end_date: str,
    factor_names: Optional[list] = None,
    instruments: Optional[list] = None,
) -> pd.DataFrame:
    names = factor_names or list(FACTOR_TABLE.keys())
    for n in names:
        assert n in FACTOR_TABLE, f"未知因子: {n}"

    cache_data = _load_cache_data(names, start_date, end_date, instruments)

    frames = []
    first_dep = next(iter(cache_data.keys()))
    for date, _ in cache_data[first_dep].groupby("date", sort=True):
        day_data = {k: v[v["date"] == date].reset_index(drop=True) for k, v in cache_data.items()}
        frames.append(_compute_factors_by_day(day_data, names))

    if not frames:
        return pd.DataFrame(columns=["date", "instrument"] + names)
    return pd.concat(frames, ignore_index=True).sort_values(["date", "instrument"]).reset_index(drop=True)


def build_pool_factors(
    pool_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    factor_names: Optional[list] = None,
    factor_weights: Optional[dict] = None,
    score_col: str = "factor_score",
) -> pd.DataFrame:
    names = factor_names or list(FACTOR_TABLE.keys())
    for n in names:
        assert n in FACTOR_TABLE, f"未知因子: {n}"

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
