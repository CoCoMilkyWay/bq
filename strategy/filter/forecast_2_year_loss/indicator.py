import json
from calendar import monthrange
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path


LOSS_TYPES = {"首亏", "续亏"}
BUFFER_TRADING_DAYS = 5
ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "tushare" / "data"
OUTPUT_PATH = Path(__file__).with_suffix(".json")


def load_json_rows(path: Path) -> list[dict]:
    rows = json.loads(path.read_text())
    assert isinstance(rows, list), f"Invalid json list: {path}"
    return rows


def iter_date_dirs() -> list[tuple[str, Path]]:
    date_dirs: list[tuple[str, Path]] = []
    for year_dir in sorted(DATA_DIR.iterdir()):
        if not year_dir.is_dir() or len(year_dir.name) != 4 or not year_dir.name.isdigit():
            continue
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or len(month_dir.name) != 2 or not month_dir.name.isdigit():
                continue
            for day_dir in sorted(month_dir.iterdir()):
                if not day_dir.is_dir() or len(day_dir.name) != 2 or not day_dir.name.isdigit():
                    continue
                date_str = f"{year_dir.name}{month_dir.name}{day_dir.name}"
                assert len(date_str) == 8 and date_str.isdigit(), f"Invalid date dir: {day_dir}"
                date_dirs.append((date_str, day_dir))
    return date_dirs


def is_year_end(end_date: str) -> bool:
    return end_date[4:6] == "12"

def is_forecast_st_row(row: dict) -> bool:
    ts_code = row.get("ts_code")
    ann_date = row.get("ann_date")
    end_date = row.get("end_date")
    forecast_type = row.get("type")
    last_parent_net = row.get("last_parent_net")

    if not ts_code or not ann_date or not end_date:
        return False
    if forecast_type not in LOSS_TYPES:
        return False
    if last_parent_net is None:
        return False

    assert isinstance(ann_date, str) and len(ann_date) == 8 and ann_date.isdigit(), f"Invalid ann_date: {row}"
    assert isinstance(end_date, str) and len(end_date) == 8 and end_date.isdigit(), f"Invalid end_date: {row}"

    if end_date[4:6] != "12":
        return False

    return float(last_parent_net) < 0


def default_release_date(end_date: str) -> str:
    release_year = int(end_date[:4]) + 1
    release_month = 4
    release_day = monthrange(release_year, release_month)[1]
    return f"{release_year}{release_month:02d}{release_day:02d}"


def add_trading_days(date_str: str, n: int) -> str:
    dt = datetime.strptime(date_str, "%Y%m%d")
    added = 0
    while added < n:
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            added += 1
    return dt.strftime("%Y%m%d")


def calc_interval_end(release_date: str, end_date: str) -> str:
    deadline = default_release_date(end_date)
    buffered = add_trading_days(release_date, BUFFER_TRADING_DAYS)
    return min(buffered, deadline)


def scan_intervals_in_date_order() -> dict[str, list[list[int]]]:
    release_date_map: dict[tuple[str, str], str] = {}
    open_start_map: dict[tuple[str, str], str] = {}
    closed_keys: set[tuple[str, str]] = set()
    intervals_by_code: dict[str, list[list[int]]] = defaultdict(list)

    for _, day_dir in iter_date_dirs():
        disclosure_path = day_dir / "disclosure.json"
        if disclosure_path.exists():
            for row in load_json_rows(disclosure_path):
                ts_code = row.get("ts_code")
                end_date = row.get("end_date")
                actual_date = row.get("actual_date")

                if not ts_code or not end_date or not actual_date:
                    continue
                assert isinstance(ts_code, str), f"Invalid ts_code: {row}"
                assert isinstance(end_date, str) and len(end_date) == 8 and end_date.isdigit(), f"Invalid end_date: {row}"
                assert isinstance(actual_date, str) and len(actual_date) == 8 and actual_date.isdigit(), f"Invalid actual_date: {row}"
                if not is_year_end(end_date):
                    continue

                key = (ts_code, end_date)
                prev_actual_date = release_date_map.get(key)
                if prev_actual_date is None or actual_date < prev_actual_date:
                    release_date_map[key] = actual_date

                if key in closed_keys:
                    continue
                start_date = open_start_map.pop(key, None)
                if start_date is None:
                    continue
                release_date = release_date_map[key]
                assert release_date >= start_date, f"release_date < start_date: {ts_code} {end_date}"
                interval_end = calc_interval_end(release_date, end_date)
                intervals_by_code[ts_code].append([int(start_date), int(interval_end)])
                closed_keys.add(key)

        forecast_path = day_dir / "forecast.json"
        if not forecast_path.exists():
            continue
        for row in load_json_rows(forecast_path):
            if not is_forecast_st_row(row):
                continue

            ts_code = row["ts_code"]
            ann_date = row["ann_date"]
            end_date = row["end_date"]
            assert isinstance(ts_code, str) and ts_code, f"Invalid ts_code: {row}"

            key = (ts_code, end_date)
            if key in closed_keys:
                continue

            release_date = release_date_map.get(key)
            if release_date is not None:
                if ann_date <= release_date:
                    interval_end = calc_interval_end(release_date, end_date)
                    intervals_by_code[ts_code].append([int(ann_date), int(interval_end)])
                    closed_keys.add(key)
                continue

            prev_start_date = open_start_map.get(key)
            if prev_start_date is None or ann_date < prev_start_date:
                open_start_map[key] = ann_date

    for (ts_code, end_date), start_date in sorted(open_start_map.items()):
        if (ts_code, end_date) in closed_keys:
            continue
        release_date = default_release_date(end_date)
        if start_date > release_date:
            continue
        intervals_by_code[ts_code].append([int(start_date), int(release_date)])

    for intervals in intervals_by_code.values():
        intervals.sort(key=lambda item: item[0])

    return dict(intervals_by_code)


def main():
    assert DATA_DIR.exists(), f"Data dir not found: {DATA_DIR}"
    intervals_by_code = scan_intervals_in_date_order()

    output = [{ts_code: intervals_by_code[ts_code]} for ts_code in sorted(intervals_by_code)]
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
