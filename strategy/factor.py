"""
因子库：市值行业中性化因子

使用方式:
    from factor import FACTORS, get_factor, get_all_factors

    # 获取单个因子
    df = get_factor('pe_ttm', '2020-01-01', '2024-12-31')

    # 获取所有因子
    df = get_all_factors('2020-01-01', '2024-12-31')

输出格式 (标准因子格式，兼容因子分析和bigquant看板):
    date, instrument, factor_name, factor_value (中性化后)
"""

from dataclasses import dataclass
from typing import Optional
import numpy as np
import pandas as pd
import dai


# ==================== 因子定义 ====================

@dataclass(frozen=True)
class FactorDef:
    name: str           # 因子名称
    field: str          # 数据库字段
    direction: int      # 1=越大越好, -1=越小越好
    desc: str           # 描述


FACTORS = {
    'pe_ttm':       FactorDef('pe_ttm',       'pe_ttm',              -1, '市盈率TTM'),
    'pb':           FactorDef('pb',           'pb',                  -1, '市净率'),
    'ps_ttm':       FactorDef('ps_ttm',       'ps_ttm',              -1, '市销率TTM'),
    'pcf_ttm':      FactorDef('pcf_ttm',      'pcf_net_ttm',         -1, '市现率TTM'),
    'roe_ttm':      FactorDef('roe_ttm',      'roe_avg_ttm',          1, 'ROE TTM'),
    'roa_ttm':      FactorDef('roa_ttm',      'roa_avg_ttm',          1, 'ROA TTM'),
    'dividend_yield': FactorDef('dividend_yield', 'dividend_yield_ratio', 1, '股息率'),
}


# ==================== 预处理函数 ====================

def winsorize_mad(s: pd.Series, n: float = 3) -> pd.Series:
    """MAD去极值"""
    valid = s.dropna()
    if len(valid) < 10:
        return s
    med = valid.median()
    mad = (valid - med).abs().median()
    if mad == 0:
        mad = valid.std() * 0.6745
    return s.clip(med - n * mad, med + n * mad)


def zscore(s: pd.Series) -> pd.Series:
    """Z-score标准化"""
    valid = s.dropna()
    if len(valid) < 10:
        return s
    mu, sigma = valid.mean(), valid.std()
    if sigma == 0:
        return s - mu
    return (s - mu) / sigma


def neutralize(factor: pd.Series, industry: pd.Series, log_mktcap: pd.Series) -> pd.Series:
    """行业+市值中性化: OLS回归取残差"""
    df = pd.DataFrame({
        'factor': factor.values,
        'industry': industry.values,
        'log_mktcap': log_mktcap.values,
    }, index=factor.index).dropna()

    if len(df) < 10 or df['industry'].nunique() < 2:
        return factor

    dummies = pd.get_dummies(df['industry'], prefix='ind', dtype=float)
    X = np.hstack([dummies.values, df[['log_mktcap']].values])
    y = df['factor'].values

    beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    residuals = y - X @ beta

    result = factor.copy()
    result.loc[df.index] = residuals
    return result


def process_cross_section(
    df: pd.DataFrame,
    factor_col: str,
    industry_col: str = 'sw2021_level1',
    mktcap_col: str = 'total_market_cap',
) -> pd.Series:
    """单截面因子处理: 去极值 -> 中性化 -> 标准化"""
    factor = df[factor_col].copy()
    factor = winsorize_mad(factor)
    factor = neutralize(factor, df[industry_col], np.log(df[mktcap_col]))
    factor = zscore(factor)
    return factor


# ==================== 数据获取 ====================

_BASE_SQL = """
SELECT
    date,
    instrument,
    total_market_cap,
    sw2021_level1,
    {fields}
FROM cn_stock_prefactors
WHERE st_status = 0
  AND suspended = 0
  AND list_days > 252
  AND total_market_cap > 0
"""

# 需要正值的因子字段 (数据库字段名)
_POSITIVE_FIELDS = {'pe_ttm', 'pb', 'ps_ttm', 'pcf_net_ttm'}


def _fetch_and_ffill(sql: str, start_date: str, end_date: str, factor_fields: list) -> pd.DataFrame:
    """获取数据并按股票 ffill"""
    df = dai.query(sql, filters={'date': [start_date, end_date]}).df()
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    df = df.sort_values(['instrument', 'date'])

    for field in factor_fields:
        df[field] = df.groupby('instrument')[field].ffill()

    return df


def _filter_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """筛选月末数据"""
    df['ym'] = df['date'].dt.to_period('M')
    month_end = df.groupby('ym')['date'].transform('max')
    return df[df['date'] == month_end].drop(columns='ym')


def _process_factor_by_date(
    df: pd.DataFrame,
    factor_field: str,
    factor_name: str,
    direction: int,
) -> pd.DataFrame:
    """按日期处理因子：去极值 -> 中性化 -> 标准化"""
    results = []
    for date, group in df.groupby('date'):
        group = group.copy()
        if factor_field in _POSITIVE_FIELDS:
            group = group[group[factor_field] > 0]
        if len(group) < 10:
            continue
        group['factor_value'] = process_cross_section(group, factor_field)
        group['factor_value'] *= direction
        results.append(group[['date', 'instrument', 'factor_value']])

    if not results:
        return pd.DataFrame(columns=['date', 'instrument', 'factor_name', 'factor_value'])
    out = pd.concat(results, ignore_index=True)
    out['factor_name'] = factor_name
    return out[['date', 'instrument', 'factor_name', 'factor_value']]


def get_factor(
    factor_name: str,
    start_date: str,
    end_date: str,
    freq: str = 'daily',
) -> pd.DataFrame:
    """
    获取单个中性化因子

    返回: DataFrame[date, instrument, factor_name, factor_value]
    """
    assert factor_name in FACTORS, f"未知因子: {factor_name}"
    fdef = FACTORS[factor_name]

    sql = _BASE_SQL.format(fields=fdef.field)
    df = _fetch_and_ffill(sql, start_date, end_date, [fdef.field])

    if freq == 'monthly':
        df = _filter_monthly(df)

    return _process_factor_by_date(df, fdef.field, factor_name, fdef.direction)


def get_all_factors(
    start_date: str,
    end_date: str,
    freq: str = 'daily',
    factor_names: Optional[list] = None,
) -> pd.DataFrame:
    """
    获取多个中性化因子

    返回: DataFrame[date, instrument, factor_name, factor_value]
    """
    names = factor_names or list(FACTORS.keys())
    fields = list({FACTORS[n].field for n in names})

    sql = _BASE_SQL.format(fields=', '.join(fields))
    df = _fetch_and_ffill(sql, start_date, end_date, fields)

    if freq == 'monthly':
        df = _filter_monthly(df)

    all_results = []
    for name in names:
        fdef = FACTORS[name]
        result = _process_factor_by_date(df.copy(), fdef.field, name, fdef.direction)
        all_results.append(result)

    return pd.concat(all_results, ignore_index=True)


def get_factor_wide(
    start_date: str,
    end_date: str,
    freq: str = 'daily',
    factor_names: Optional[list] = None,
) -> pd.DataFrame:
    """
    获取宽表格式因子 (用于策略排序)

    返回: DataFrame[date, instrument, pe_ttm, pb, ...]
    """
    names = factor_names or list(FACTORS.keys())
    fields = list({FACTORS[n].field for n in names})

    sql = _BASE_SQL.format(fields=', '.join(fields))
    df = _fetch_and_ffill(sql, start_date, end_date, fields)

    if freq == 'monthly':
        df = _filter_monthly(df)

    results = []
    for date, group in df.groupby('date'):
        group = group.copy()
        for name in names:
            fdef = FACTORS[name]
            valid_mask = pd.Series(True, index=group.index)
            if fdef.field in _POSITIVE_FIELDS:
                valid_mask = group[fdef.field] > 0
            group[name] = np.nan
            if valid_mask.sum() >= 10:
                valid_group = group.loc[valid_mask].copy()
                valid_group[name] = process_cross_section(valid_group, fdef.field)
                valid_group[name] *= fdef.direction
                group.loc[valid_mask, name] = valid_group[name]
        results.append(group[['date', 'instrument'] + names])

    return pd.concat(results, ignore_index=True)
