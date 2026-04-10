import json
from pathlib import Path
from collections import defaultdict

from update_bulk import DATA_DIR  # pyright: ignore[reportMissingImports]

DATA_TYPES = {
    'disclosure': 'actual_date',
    'forecast': 'ann_date',
    'express': 'ann_date',
}

QUARTER_END_MONTHS = {'03': 'Q1', '06': 'Q2', '09': 'Q3', '12': 'Q4'}

MAX_OFFSET = 5


def parse_period(end_date: str) -> tuple[int, str] | None:
    """Parse end_date to (year, quarter). Returns None if invalid."""
    if not end_date or len(end_date) != 8:
        return None
    year = int(end_date[:4])
    month = end_date[4:6]
    quarter = QUARTER_END_MONTHS.get(month)
    if quarter is None:
        return None
    return (year, quarter)


def calc_offset(end_date: str, pub_date: str) -> int | None:
    """Calculate month offset from period end to publication date."""
    if not end_date or not pub_date or len(end_date) != 8 or len(pub_date) != 8:
        return None
    end_year, end_month = int(end_date[:4]), int(end_date[4:6])
    pub_year, pub_month = int(pub_date[:4]), int(pub_date[4:6])
    return (pub_year - end_year) * 12 + (pub_month - end_month)


def load_all_data() -> dict:
    """Load all JSON data, returns {data_type: [(ts_code, end_date, pub_date), ...]}
    
    Deduplicates by (ts_code, end_date), keeping the latest pub_date.
    """
    raw = {dt: {} for dt in DATA_TYPES}  # {data_type: {(ts_code, end_date): pub_date}}

    for year_dir in sorted(DATA_DIR.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for month_dir in year_dir.iterdir():
            if not month_dir.is_dir():
                continue
            for day_dir in month_dir.iterdir():
                if not day_dir.is_dir():
                    continue
                for data_type, pub_field in DATA_TYPES.items():
                    json_path = day_dir / f'{data_type}.json'
                    if not json_path.exists():
                        continue
                    records = json.loads(json_path.read_text())
                    for r in records:
                        ts_code = r.get('ts_code')
                        end_date = r.get('end_date')
                        pub_date = r.get(pub_field)
                        if ts_code and end_date and pub_date:
                            key = (ts_code, end_date)
                            if key not in raw[data_type] or pub_date > raw[data_type][key]:
                                raw[data_type][key] = pub_date

    result = {dt: [(k[0], k[1], v) for k, v in d.items()] for dt, d in raw.items()}
    return result


def aggregate_coverage(data: dict) -> dict:
    """
    Aggregate to: {(year, quarter): {data_type: {offset_bin: set(ts_codes)}}}
    offset_bin: '<0', 0, 1, 2, 3, 4, 5, '>5'
    """
    result = defaultdict(lambda: {dt: defaultdict(set) for dt in DATA_TYPES})

    for data_type, records in data.items():
        for ts_code, end_date, pub_date in records:
            period = parse_period(end_date)
            if period is None:
                continue
            offset = calc_offset(end_date, pub_date)

            if offset is None:
                offset_bin = 'NA'
            elif offset < 0:
                offset_bin = '<0'
            elif offset > MAX_OFFSET:
                offset_bin = f'>{MAX_OFFSET}'
            else:
                offset_bin = offset

            result[period][data_type][offset_bin].add(ts_code)

    return result


def fmt_num(n: int, width: int) -> str:
    return f'{"-":>{width}}' if n == 0 else f'{n:>{width}}'


def print_table(agg: dict):
    """Print the coverage table with hierarchy: each period has 3 rows (D/F/E)."""
    offset_bins = ['<0'] + list(range(MAX_OFFSET + 1)) + [f'>{MAX_OFFSET}']
    col_width = 6

    periods = sorted(agg.keys())
    if not periods:
        print('No data found.')
        return

    header_cols = [f'{"Period":>7}', f'{"Type":>4}', f'{"Total":>5}']
    header_cols += [f'{str(b):>{col_width}}' for b in offset_bins]
    header = '|'.join(header_cols) + '|'
    sep = '|'.join('-' * len(c) for c in header_cols) + '|'

    print(header)
    print(sep)

    for period in periods:
        year, quarter = period
        period_str = f'{year} {quarter}'

        for i, data_type in enumerate(['disclosure', 'forecast', 'express']):
            type_char = data_type[0].upper()
            counts = agg[period][data_type]

            total = len(set().union(*counts.values())) if counts else 0

            cells = [fmt_num(len(counts.get(b, set())), col_width) for b in offset_bins]

            if i == 0:
                row = f'{period_str:>7}|{type_char:>4}|{total:>5}|' + '|'.join(cells) + '|'
            else:
                row = f'{"":>7}|{type_char:>4}|{total:>5}|' + '|'.join(cells) + '|'
            print(row)

        print(sep)


def main():
    print(f'Loading data from {DATA_DIR}...')
    data = load_all_data()
    for dt, records in data.items():
        print(f'  {dt}: {len(records)} records')

    print('\nAggregating...')
    agg = aggregate_coverage(data)
    print(f'  {len(agg)} periods\n')

    print('Coverage Table (D=disclosure, F=forecast, E=express)')
    print('Columns: offset months from period end (e.g., +1 = 1 month after quarter end)\n')
    print_table(agg)


if __name__ == '__main__':
    main()
