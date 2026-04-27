import tushare as ts
from datetime import date

pro = ts.pro_api()
ts.set_token('439b79afc0af96f0abb32a3be27df99b9e8fe9fa83f8d555d66fba72')

TODAY = date.today().strftime('%Y%m%d')
YEAR_REPORT = '20251231'  # 去年年报
Q1_REPORT = '20260331'    # 今年一季报

POOLS = {
    '融资': ['301139', '301126', '600448', '001387', '301037', '001373', '000056', '300923', '301512', '002909', '001277', '301006', '301353', '301390', '301131'],
    '低估值': ['600697', '301139', '603729', '600476', '301037', '300417', '600561', '300670', '600448', '300823', '301126', '605567', '300621', '002524', '603214'],
    '低价': ['600303', '002513', '600778', '002591', '603022', '600561', '002486', '002809', '002247', '600493', '002551', '600287', '002495', '603329', '600802'],
}

def to_ts_code(code: str) -> str:
    if code.startswith('6'):
        return f'{code}.SH'
    return f'{code}.SZ'

def get_stock_names(ts_codes: list[str]) -> dict[str, str]:
    """批量获取股票名称"""
    df = pro.stock_basic(fields='ts_code,name')
    name_map = dict(zip(df['ts_code'], df['name']))
    return {code: name_map.get(code, '?') for code in ts_codes}

def calc_pending_days(pre_date: str | None, actual_date: str | None) -> str:
    from datetime import datetime
    if actual_date:
        return '已披露'
    if not pre_date:
        return '无计划'
    pre = datetime.strptime(pre_date, '%Y%m%d')
    today = datetime.strptime(TODAY, '%Y%m%d')
    delta = (pre - today).days
    if delta < 0:
        return f'超期{-delta}天'
    return f'还剩{delta}天'

def get_disclosure_info(ts_code: str, period: str) -> dict:
    df = pro.disclosure_date(ts_code=ts_code, end_date=period)
    if df.empty:
        return {'pre_date': None, 'actual_date': None}
    row = df.iloc[0]
    return {
        'pre_date': row.get('pre_date'),
        'actual_date': row.get('actual_date'),
    }

def get_forecast_info(ts_code: str, period: str) -> dict | None:
    df = pro.forecast(ts_code=ts_code, period=period)
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        'ann_date': row.get('ann_date'),
        'type': row.get('type'),
        'net_profit_min': row.get('net_profit_min'),
        'net_profit_max': row.get('net_profit_max'),
        'last_parent_net': row.get('last_parent_net'),
        'p_change_min': row.get('p_change_min'),
        'p_change_max': row.get('p_change_max'),
    }

def get_express_info(ts_code: str, period: str) -> dict | None:
    df = pro.express(ts_code=ts_code, period=period)
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        'ann_date': row.get('ann_date'),
        'n_income': row.get('n_income'),  # 净利润(元)
        'np_last_year': row.get('np_last_year'),  # 去年同期净利润
        'yoy_dedu_np': row.get('yoy_dedu_np'),  # 同比增长率
    }

def get_income_info(ts_code: str, period: str) -> dict | None:
    df = pro.income(ts_code=ts_code, period=period, report_type='1')
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        'ann_date': row.get('f_ann_date') or row.get('ann_date'),
        'n_income_attr_p': row.get('n_income_attr_p'),  # 归母净利润(元)
    }

def fmt_profit(v) -> str:
    """v单位是元，转成亿元显示"""
    if v is None:
        return '-'
    v = v / 1e8  # 元转亿元
    return f'{v:.2f}亿'

def fmt_profit_wan(v) -> str:
    """v单位是万元，转成亿元显示"""
    if v is None:
        return '-'
    v = v / 1e4  # 万元转亿元
    return f'{v:.2f}亿'

def fmt_change(cur, prev) -> str:
    if cur is None or prev is None or prev == 0:
        return '-'
    chg = (cur - prev) / abs(prev) * 100
    return f'{chg:+.1f}%'

def analyze_stock(ts_code: str, period: str, prev_period: str) -> dict:
    disc = get_disclosure_info(ts_code, period)
    forecast = get_forecast_info(ts_code, period)
    express = get_express_info(ts_code, period)
    income = get_income_info(ts_code, period)
    prev_income = get_income_info(ts_code, prev_period)
    
    return {
        'disclosure': disc,
        'forecast': forecast,
        'express': express,
        'income': income,
        'prev_income': prev_income,
    }

def pad(s: str, width: int) -> str:
    """按显示宽度左对齐填充（中文按2宽度）"""
    actual = sum(2 if ord(c) > 127 else 1 for c in s)
    return s + ' ' * max(1, width - actual)

def fmt_section(info: dict) -> tuple[str, str, str]:
    """返回 (披露状态, 源, 利润对比) 一段紧凑展示"""
    disc = info['disclosure']
    forecast = info['forecast']
    express = info['express']
    income = info['income']
    prev_income = info['prev_income']
    
    pending = calc_pending_days(disc['pre_date'], disc['actual_date'])
    prev_profit = prev_income['n_income_attr_p'] if prev_income else None
    
    if income:
        cur = income['n_income_attr_p']
        return pending, '报告', f'{fmt_profit(cur)} vs {fmt_profit(prev_profit)} ({fmt_change(cur, prev_profit)})'
    if express:
        cur = express['n_income']
        prev_e = express.get('np_last_year') or prev_profit
        return pending, '快报', f'{fmt_profit(cur)} vs {fmt_profit(prev_e)} ({fmt_change(cur, prev_e)})'
    if forecast:
        if forecast['net_profit_min'] is not None and forecast['net_profit_max'] is not None:
            cur_wan = (forecast['net_profit_min'] + forecast['net_profit_max']) / 2
        else:
            cur_wan = forecast.get('net_profit_min') or forecast.get('net_profit_max')
        last_wan = forecast.get('last_parent_net')
        s = f'{fmt_profit_wan(cur_wan)} vs {fmt_profit_wan(last_wan)}'
        if forecast.get('p_change_min') is not None and forecast.get('p_change_max') is not None:
            s += f' ({forecast["p_change_min"]:+.0f}%~{forecast["p_change_max"]:+.0f}%)'
        return pending, forecast['type'] or '预告', s
    return pending, '-', '-'

# 列宽: code, name, 年报状态, 年报源, 年报利润, Q1状态, Q1源, Q1利润
COLS = (8, 14, 12, 6, 36, 12, 6, 36)

def fmt_row(cells: tuple) -> str:
    return (
        f'{pad(cells[0], COLS[0])}{pad(cells[1], COLS[1])}'
        f'│ {pad(cells[2], COLS[2])}{pad(cells[3], COLS[3])}{pad(cells[4], COLS[4])}'
        f'│ {pad(cells[5], COLS[5])}{pad(cells[6], COLS[6])}{pad(cells[7], COLS[7])}'
    )

def main():
    print(f'今日: {TODAY}  查询: 年报={YEAR_REPORT}  Q1={Q1_REPORT}')
    
    all_codes = [c for codes in POOLS.values() for c in codes]
    all_ts_codes = [to_ts_code(c) for c in all_codes]
    names = get_stock_names(all_ts_codes)
    
    header = fmt_row(('代码', '名称', '年报披露', '源', '利润对比', 'Q1披露', '源', '利润对比'))
    sep = '─' * (sum(COLS) + 4)
    
    for pool_name, codes in POOLS.items():
        print(f'\n═══ {pool_name} ═══')
        print(header, flush=True)
        print(sep, flush=True)
        for code in codes:
            ts_code = to_ts_code(code)
            name = names.get(ts_code, '?')
            info_ar = analyze_stock(ts_code, YEAR_REPORT, '20241231')
            info_q1 = analyze_stock(ts_code, Q1_REPORT, '20250331')
            ar = fmt_section(info_ar)
            q1 = fmt_section(info_q1)
            print(fmt_row((code, name, ar[0], ar[1], ar[2], q1[0], q1[1], q1[2])), flush=True)

if __name__ == '__main__':
    main()
