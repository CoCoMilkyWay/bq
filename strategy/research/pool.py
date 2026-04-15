import tushare as ts
from datetime import date

pro = ts.pro_api()
ts.set_token('439b79afc0af96f0abb32a3be27df99b9e8fe9fa83f8d555d66fba72')

TODAY = date.today().strftime('%Y%m%d')
YEAR_REPORT = '20251231'  # 去年年报
Q1_REPORT = '20260331'    # 今年一季报

POOLS = {
    '融资': ['600379', '600470', '001324', '600293', '603335', '301163', '301272', '301006', '001373', '301512'],
    '低估值': ['301167', '300886', '300670', '300417', '300823', '001211', '603729', '301139', '600476', '600561'],
    '低价': ['002486', '600689', '002495', '002809', '603022', '600778', '603335', '002652', '603137', '600202'],
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

def print_stock_info(code: str, name: str, period_label: str, info: dict):
    disc = info['disclosure']
    forecast = info['forecast']
    express = info['express']
    income = info['income']
    prev_income = info['prev_income']
    
    pending = calc_pending_days(disc['pre_date'], disc['actual_date'])
    pre_date = disc['pre_date'] or '-'
    actual_date = disc['actual_date'] or '-'
    
    # 净利润对比 - 优先级: 正式报告 > 业绩快报 > 业绩预告
    cur_profit = None
    prev_profit = prev_income['n_income_attr_p'] if prev_income else None
    source = ''
    
    if income:
        cur_profit = income['n_income_attr_p']
        source = '正式报告'
        profit_str = f'{fmt_profit(cur_profit)} vs {fmt_profit(prev_profit)} ({fmt_change(cur_profit, prev_profit)})'
    elif express:
        cur_profit = express['n_income']
        prev_from_express = express.get('np_last_year')
        source = '业绩快报'
        # 优先用express自带的去年同期
        if prev_from_express:
            profit_str = f'{fmt_profit(cur_profit)} vs {fmt_profit(prev_from_express)} ({fmt_change(cur_profit, prev_from_express)})'
        else:
            profit_str = f'{fmt_profit(cur_profit)} vs {fmt_profit(prev_profit)} ({fmt_change(cur_profit, prev_profit)})'
    elif forecast:
        # 用预告的平均值 (单位是万元)
        if forecast['net_profit_min'] is not None and forecast['net_profit_max'] is not None:
            cur_profit_wan = (forecast['net_profit_min'] + forecast['net_profit_max']) / 2
        else:
            cur_profit_wan = forecast.get('net_profit_min') or forecast.get('net_profit_max')
        last_wan = forecast.get('last_parent_net')  # 上年同期(万元)
        source = f"业绩预告({forecast['type']})"
        profit_str = f'{fmt_profit_wan(cur_profit_wan)} vs {fmt_profit_wan(last_wan)}'
        if forecast.get('p_change_min') is not None and forecast.get('p_change_max') is not None:
            chg_min, chg_max = forecast['p_change_min'], forecast['p_change_max']
            profit_str += f' ({chg_min:+.1f}%~{chg_max:+.1f}%)'
    else:
        source = '未披露'
        profit_str = '-'
    
    print(f'  预计{pre_date} 实际{actual_date} | {pending} | {source}: {profit_str}')

def main():
    print(f'今日: {TODAY}')
    print(f'查询: 2025年报({YEAR_REPORT}) + 2026Q1({Q1_REPORT})\n')
    
    # 收集所有股票代码并获取名称
    all_codes = []
    for codes in POOLS.values():
        all_codes.extend(codes)
    all_ts_codes = [to_ts_code(c) for c in all_codes]
    names = get_stock_names(all_ts_codes)
    
    for pool_name, codes in POOLS.items():
        print(f'=== {pool_name} ===')
        for code in codes:
            ts_code = to_ts_code(code)
            name = names.get(ts_code, '?')
            
            # 2025年报 vs 2024年报
            info_ar = analyze_stock(ts_code, YEAR_REPORT, '20241231')
            print(f'{code} {name} 2025年报:')
            print_stock_info(code, name, '2025年报', info_ar)
            
            # 2026Q1 vs 2025Q1
            info_q1 = analyze_stock(ts_code, Q1_REPORT, '20250331')
            print(f'{code} {name} 2026Q1:')
            print_stock_info(code, name, '2026Q1', info_q1)
            print()
        print()

if __name__ == '__main__':
    main()
