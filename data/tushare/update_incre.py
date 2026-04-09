import tushare as ts
from pathlib import Path
from datetime import date, datetime, timedelta

from update_bulk import (  # pyright: ignore[reportMissingImports]
    DATA_DIR, RANGE_INTERFACES, DAILY_INTERFACES,
    save_df_by_visible_date
)

pro = ts.pro_api()


def find_last_date() -> str:
    """扫描 DATA_DIR 找到最后一天有数据的日期"""
    dates = []
    for year_dir in DATA_DIR.iterdir():
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for month_dir in year_dir.iterdir():
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue
            for day_dir in month_dir.iterdir():
                if not day_dir.is_dir() or not day_dir.name.isdigit():
                    continue
                date_str = f'{year_dir.name}{month_dir.name}{day_dir.name}'
                dates.append(date_str)
    assert dates, 'No data found'
    return max(dates)


def gen_daily_dates(start: str, end: str) -> list[str]:
    start_dt = datetime.strptime(start, '%Y%m%d')
    end_dt = datetime.strptime(end, '%Y%m%d')
    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    return dates


def fetch_range_daily(interface: str, output_name: str, date_fields: list[str], dates: list[str]):
    total = len(dates)
    print(f'\n{interface}')
    for i, d in enumerate(dates, 1):
        print(f'  [{i}/{total}] {d}', end=' ', flush=True)

        df = getattr(pro, interface)(start_date=d, end_date=d)
        assert df is not None, f'Failed to fetch {interface} for {d}'

        if df.empty:
            print('-> 0 records')
            continue

        save_df_by_visible_date(df, output_name, date_fields, d, d)
        print(f'-> {len(df)} records')


def fetch_daily(interface: str, output_name: str, query_param: str, date_fields: list[str], dates: list[str]):
    total = len(dates)
    print(f'\n{interface}')
    for i, d in enumerate(dates, 1):
        print(f'  [{i}/{total}] {d}', end=' ', flush=True)

        df = getattr(pro, interface)(**{query_param: d})
        assert df is not None, f'Failed to fetch {interface} for {d}'

        if df.empty:
            print('-> 0 records')
            continue

        save_df_by_visible_date(df, output_name, date_fields, d, d)
        print(f'-> {len(df)} records')


def main():
    last_date = find_last_date()
    last_dt = datetime.strptime(last_date, '%Y%m%d')
    start_dt = last_dt + timedelta(days=1)
    start = start_dt.strftime('%Y%m%d')
    end = date.today().strftime('%Y%m%d')

    if start > end:
        print(f'Already up to date (last: {last_date})')
        return

    dates = gen_daily_dates(start, end)

    print(f'Last date: {last_date}')
    print(f'Update range: {start} ~ {end}')
    print(f'Days: {len(dates)}')

    for interface, output_name, date_fields in RANGE_INTERFACES:
        fetch_range_daily(interface, output_name, date_fields, dates)

    for interface, output_name, query_param, date_fields in DAILY_INTERFACES:
        fetch_daily(interface, output_name, query_param, date_fields, dates)


if __name__ == '__main__':
    main()
