import json
from calendar import monthrange
from collections import defaultdict
from pathlib import Path

import dai
import pandas as pd


LOSS_TYPES = {"首亏", "续亏"}
ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data" / "tushare" / "data"
OUTPUT_PATH = Path(__file__).with_suffix(".json")

LIST_SECTOR_MAIN_BOARD = 1
REVENUE_THRESHOLD_DEFAULT = 1e8
REVENUE_THRESHOLD_MAIN_BOARD_2024 = 3e8


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


def ts_code_to_instrument(ts_code: str) -> str:
    return ts_code.upper()


def fetch_ttm_revenue() -> dict[tuple[str, str], float]:
    """
    获取 TTM 营收数据 (point-in-time)
    返回: {(instrument, date_str): revenue_ttm}
    date_str 格式: YYYYMMDD
    """
    sql = """
    SELECT date, instrument, total_operating_revenue_ttm
    FROM cn_stock_financial_ttm_shift
    WHERE shift = 0
    """
    df = dai.query(sql, full_db_scan=True).df()
    df["date"] = pd.to_datetime(df["date"])
    df["date_str"] = df["date"].dt.strftime("%Y%m%d")
    
    revenue_map = {}
    for _, row in df.iterrows():
        key = (row["instrument"], row["date_str"])
        revenue = row["total_operating_revenue_ttm"]
        if pd.notna(revenue):
            revenue_map[key] = float(revenue)
    return revenue_map


def get_latest_ttm_revenue(
    instrument: str,
    as_of_date: str,
    revenue_map: dict[tuple[str, str], float],
    all_dates: list[str],
) -> float | None:
    """
    获取截至 as_of_date 的最新可用 TTM 营收
    """
    for d in reversed(all_dates):
        if d > as_of_date:
            continue
        key = (instrument, d)
        if key in revenue_map:
            return revenue_map[key]
    return None


def fetch_sector_map() -> dict[str, int]:
    """
    获取股票板块信息
    返回: {instrument: list_sector}
    """
    sql = """
    SELECT instrument, list_sector
    FROM cn_stock_basic_info
    """
    df = dai.query(sql, full_db_scan=True).df()
    return {row["instrument"]: int(row["list_sector"]) 
            for _, row in df.iterrows() if pd.notna(row["list_sector"])}


def get_revenue_threshold(end_date: str, sector: int | None) -> float:
    report_year = int(end_date[:4])
    is_main_board = (sector == LIST_SECTOR_MAIN_BOARD)
    if report_year >= 2024 and is_main_board:
        return REVENUE_THRESHOLD_MAIN_BOARD_2024
    return REVENUE_THRESHOLD_DEFAULT


def is_forecast_st_row(
    row: dict, 
    ttm_revenue_map: dict[tuple[str, str], float],
    all_ttm_dates: list[str],
    sector_map: dict[str, int]
) -> bool:
    ts_code = row.get("ts_code")
    ann_date = row.get("ann_date")
    end_date = row.get("end_date")
    forecast_type = row.get("type")

    if not ts_code or not ann_date or not end_date:
        return False
    
    assert isinstance(ann_date, str) and len(ann_date) == 8 and ann_date.isdigit(), f"Invalid ann_date: {row}"
    assert isinstance(end_date, str) and len(end_date) == 8 and end_date.isdigit(), f"Invalid end_date: {row}"

    if not is_year_end(end_date):
        return False
    
    report_year = int(end_date[:4])
    # 21年后新规才适用: 营收<阈值 + 亏损 -> ST
    if report_year < 2021:
        return False
    
    # 公告日期必须在21年1月1日之后 (新规生效后)
    if ann_date < "20210101":
        return False
    
    if forecast_type not in LOSS_TYPES:
        return False
    
    instrument = ts_code_to_instrument(ts_code)
    
    # 使用 ann_date 时可用的最新 TTM 营收
    revenue = get_latest_ttm_revenue(instrument, ann_date, ttm_revenue_map, all_ttm_dates)
    if revenue is None:
        return False
    
    sector = sector_map.get(instrument)
    threshold = get_revenue_threshold(end_date, sector)
    
    return revenue < threshold


def default_release_date(end_date: str) -> str:
    release_year = int(end_date[:4]) + 1
    release_month = 4
    release_day = monthrange(release_year, release_month)[1]
    return f"{release_year}{release_month:02d}{release_day:02d}"


def scan_intervals_in_date_order(
    ttm_revenue_map: dict[tuple[str, str], float],
    all_ttm_dates: list[str],
    sector_map: dict[str, int]
) -> dict[str, list[list[int]]]:
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
                intervals_by_code[ts_code].append([int(start_date), int(release_date)])
                closed_keys.add(key)

        forecast_path = day_dir / "forecast.json"
        if not forecast_path.exists():
            continue
        for row in load_json_rows(forecast_path):
            if not is_forecast_st_row(row, ttm_revenue_map, all_ttm_dates, sector_map):
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
                    intervals_by_code[ts_code].append([int(ann_date), int(release_date)])
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
    
    print("Fetching TTM revenue data from bigquant...")
    ttm_revenue_map = fetch_ttm_revenue()
    all_ttm_dates = sorted(set(d for _, d in ttm_revenue_map.keys()))
    print(f"  Loaded {len(ttm_revenue_map)} TTM revenue records, {len(all_ttm_dates)} dates")
    
    print("Fetching sector data from bigquant...")
    sector_map = fetch_sector_map()
    print(f"  Loaded {len(sector_map)} sector records")
    
    print("Scanning forecast intervals...")
    intervals_by_code = scan_intervals_in_date_order(ttm_revenue_map, all_ttm_dates, sector_map)
    print(f"  Found {len(intervals_by_code)} instruments with forecast_st intervals")
    
    output = [{ts_code: intervals_by_code[ts_code]} for ts_code in sorted(intervals_by_code)]
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"Output written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
