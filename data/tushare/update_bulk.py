import json
import tushare as ts
from pathlib import Path
from datetime import date, timedelta

pro = ts.pro_api()

DATA_DIR = Path(__file__).parent / 'data'

BULK_INTERFACES = [
    # (接口名, 输出文件名, 公告日期字段优先级)
    ('income_vip', 'income', ['f_ann_date', 'ann_date']),
    ('balancesheet_vip', 'balancesheet', ['f_ann_date', 'ann_date']),
    ('cashflow_vip', 'cashflow', ['f_ann_date', 'ann_date']),
    ('forecast_vip', 'forecast', ['ann_date']),
    ('express_vip', 'express', ['ann_date']),
    ('fina_indicator_vip', 'fina_indicator', ['ann_date']),
    ('disclosure_date', 'disclosure_date', ['actual_date']),
]

# 不支持的接口:
# - fina_mainbz_vip: 无公告日期字段，只有 end_date（报告期）
# - fina_audit: ts_code 必选，无 VIP 接口，无法批量拉取
# - dividend: 无 start_date/end_date 参数

# 年报：period=20231231 + report_type=1（全年累计）
# 四季报：period=20231231 + report_type=2（仅Q4单季）
# 半年报：period=20230630 + report_type=1（仅H1累计）
# 季报：period=20230630 + report_type=2（仅Q2单季）

def gen_date_ranges(start: str, end: str, days: int = 30) -> list[tuple[str, str]]:
    start_date = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
    end_date = date(int(end[:4]), int(end[4:6]), int(end[6:8]))
    
    ranges = []
    current = start_date
    while current <= end_date:
        range_end = min(current + timedelta(days=days - 1), end_date)
        ranges.append((current.strftime('%Y%m%d'), range_end.strftime('%Y%m%d')))
        current = range_end + timedelta(days=1)
    return ranges


def append_to_json(path: Path, records: list):
    existing = []
    if path.exists():
        existing = json.loads(path.read_text())
    existing.extend(records)
    path.write_text(json.dumps(existing, ensure_ascii=False, indent=2))


def get_visible_date(row, date_fields: list[str]) -> str | None:
    for field in date_fields:
        if row.get(field):
            return row[field]
    return None


def save_df_by_visible_date(df, output_name: str, date_fields: list[str]) -> tuple[set[str], int]:
    df = df.copy()
    df['_visible_date'] = df.apply(lambda r: get_visible_date(r, date_fields), axis=1)
    
    skipped = df['_visible_date'].isna().sum()
    df = df[df['_visible_date'].notna()]
    
    dates_written = set()
    for visible_date, group in df.groupby('_visible_date'):
        output_dir = DATA_DIR / visible_date[:4] / visible_date[4:6] / visible_date[6:8]
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f'{output_name}.json'
        
        records = group.drop(columns=['_visible_date']).to_dict('records')
        append_to_json(output_path, records)
        dates_written.add(visible_date)
    
    return dates_written, skipped


def fetch_and_save(interface: str, output_name: str, date_fields: list[str], date_ranges: list[tuple[str, str]], interface_idx: int, total_interfaces: int):
    total_ranges = len(date_ranges)
    
    print(f'\n[{interface_idx}/{total_interfaces}] {interface}')
    
    for i, (start_date, end_date) in enumerate(date_ranges, 1):
        print(f'  [{i}/{total_ranges}] {start_date}~{end_date}', end=' ', flush=True)
        
        df = getattr(pro, interface)(start_date=start_date, end_date=end_date)
        assert df is not None, f'Failed to fetch {interface} for {start_date}~{end_date}'
        
        if df.empty:
            print('-> 0 records')
            continue
        
        dates_written, skipped = save_df_by_visible_date(df, output_name, date_fields)
        
        if dates_written:
            date_range = f'{min(dates_written)} ~ {max(dates_written)}'
        else:
            date_range = 'N/A'
        
        skip_info = f', skipped {skipped}' if skipped else ''
        print(f'-> {len(df)} records, {len(dates_written)} dates ({date_range}){skip_info}')


def main():
    start = '20250101'
    end = date.today().strftime('%Y%m%d')
    
    date_ranges = gen_date_ranges(start, end, days=30)
    total_interfaces = len(BULK_INTERFACES)
    
    print(f'Output dir: {DATA_DIR}')
    print(f'Date range: {start} ~ {end}')
    print(f'Chunks: {len(date_ranges)} (30 days each)')
    print(f'Interfaces: {total_interfaces}')
    print(f'Total requests: {len(date_ranges) * total_interfaces}')
    
    for i, (interface, output_name, date_fields) in enumerate(BULK_INTERFACES, 1):
        fetch_and_save(interface, output_name, date_fields, date_ranges, i, total_interfaces)


if __name__ == '__main__':
    main()
