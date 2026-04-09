import json
import tushare as ts
from pathlib import Path
from datetime import date

pro = ts.pro_api()

DATA_DIR = Path(__file__).parent / 'data'

START_DATE = '20150101'


RANGE_INTERFACES = [
    # (接口名, 输出文件名, visible_date字段)
    # 这些接口支持 start_date/end_date 范围查询
    ('forecast_vip', 'forecast', ['ann_date']),
    ('express_vip', 'express', ['ann_date']),
]

DAILY_INTERFACES = [
    # (接口名, 输出文件名, 查询参数名, visible_date字段)
    # 这些接口只支持单日期参数查询
    ('disclosure_date', 'disclosure', 'actual_date', ['actual_date']),
]

# 年报：period=20231231 + report_type=1（全年累计）
# 四季报：period=20231231 + report_type=2（仅Q4单季）
# 半年报：period=20230630 + report_type=1（仅H1累计）
# 季报：period=20230630 + report_type=2（仅Q2单季）


def gen_monthly_ranges(start: str, end: str) -> list[tuple[str, str]]:
    from calendar import monthrange

    start_year, start_month = int(start[:4]), int(start[4:6])
    end_year, end_month = int(end[:4]), int(end[4:6])

    ranges = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        month_start = f'{year}{month:02d}01'
        month_end = f'{year}{month:02d}{monthrange(year, month)[1]}'
        ranges.append((month_start, month_end))

        month += 1
        if month > 12:
            month = 1
            year += 1
    return ranges


def append_to_json(path: Path, records: list):
    import math
    def clean_nan(v):
        return None if isinstance(v, float) and math.isnan(v) else v
    records = [{k: clean_nan(v) for k, v in r.items()} for r in records]

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


def save_df_by_visible_date(df, output_name: str, date_fields: list[str], start_date: str, end_date: str) -> tuple[set[str], int, int]:
    df = df.copy()
    df['_visible_date'] = df.apply(
        lambda r: get_visible_date(r, date_fields), axis=1)

    skip_no_date = df['_visible_date'].isna().sum()
    df = df[df['_visible_date'].notna()]

    skip_out_of_range = ((df['_visible_date'] < start_date) | (
        df['_visible_date'] > end_date)).sum()
    df = df[(df['_visible_date'] >= start_date)
            & (df['_visible_date'] <= end_date)]

    dates_written = set()
    for visible_date, group in df.groupby('_visible_date'):
        output_dir = DATA_DIR / \
            visible_date[:4] / visible_date[4:6] / visible_date[6:8]
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f'{output_name}.json'

        records = group.drop(columns=['_visible_date']).to_dict('records')
        append_to_json(output_path, records)
        dates_written.add(visible_date)

    return dates_written, skip_no_date, skip_out_of_range


def gen_daily_dates(start: str, end: str) -> list[str]:
    from datetime import datetime, timedelta
    start_dt = datetime.strptime(start, '%Y%m%d')
    end_dt = datetime.strptime(end, '%Y%m%d')
    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    return dates


def fetch_and_save_range(interface: str, output_name: str, date_fields: list[str], date_ranges: list[tuple[str, str]]):
    total_ranges = len(date_ranges)
    print(f'\n{interface}')

    for i, (start_date, end_date) in enumerate(date_ranges, 1):
        print(f'  [{i}/{total_ranges}] {start_date}~{end_date}',
              end=' ', flush=True)

        df = getattr(pro, interface)(start_date=start_date, end_date=end_date)
        assert df is not None, f'Failed to fetch {interface} for {start_date}~{end_date}'

        if df.empty:
            print('-> 0 records')
            continue

        dates_written, skip_no_date, skip_out_of_range = save_df_by_visible_date(
            df, output_name, date_fields, start_date, end_date)

        if dates_written:
            date_range = f'{min(dates_written)} ~ {max(dates_written)}'
        else:
            date_range = 'N/A'

        skip_parts = []
        if skip_no_date:
            skip_parts.append(f'no_date={skip_no_date}')
        if skip_out_of_range:
            skip_parts.append(f'out_of_range={skip_out_of_range}')
        skip_info = f', skip {"+".join(skip_parts)}' if skip_parts else ''
        print(
            f'-> {len(df)} records, {len(dates_written)} dates ({date_range}){skip_info}')


def fetch_and_save_daily(interface: str, output_name: str, query_param: str, date_fields: list[str], dates: list[str]):
    total = len(dates)
    print(f'\n{interface}')

    for i, d in enumerate(dates, 1):
        print(f'  [{i}/{total}] {d}', end=' ', flush=True)

        df = getattr(pro, interface)(**{query_param: d})
        assert df is not None, f'Failed to fetch {interface} for {d}'

        if df.empty:
            print('-> 0 records')
            continue

        dates_written, _, _ = save_df_by_visible_date(df, output_name, date_fields, d, d)
        print(f'-> {len(df)} records')


def main():
    END_DATE = date.today().strftime('%Y%m%d')

    print(f'Output dir: {DATA_DIR}')
    print(f'Date range: {START_DATE} ~ {END_DATE}')

    monthly_ranges = gen_monthly_ranges(START_DATE, END_DATE)
    for interface, output_name, date_fields in RANGE_INTERFACES:
        fetch_and_save_range(interface, output_name, date_fields, monthly_ranges)

    daily_dates = gen_daily_dates(START_DATE, END_DATE)
    for interface, output_name, query_param, date_fields in DAILY_INTERFACES:
        fetch_and_save_daily(interface, output_name, query_param, date_fields, daily_dates)


if __name__ == '__main__':
    main()
