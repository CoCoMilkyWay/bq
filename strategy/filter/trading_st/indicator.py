import json
from collections import defaultdict
from pathlib import Path

import dai
import pandas as pd


OUTPUT_PATH = Path(__file__).with_suffix(".json")

CONSECUTIVE_DAYS = 20
MARKET_CAP_THRESHOLD_MAIN = 5e8
MARKET_CAP_THRESHOLD_OTHER = 3e8
PRICE_THRESHOLD = 1.0
LIST_SECTOR_MAIN_BOARD = 1


def fetch_data() -> pd.DataFrame:
    sql = """
    SELECT date, instrument, close, total_market_cap, list_sector
    FROM cn_stock_prefactors
    WHERE close IS NOT NULL AND total_market_cap IS NOT NULL
    ORDER BY instrument, date
    """
    df = dai.query(sql, full_db_scan=True).df()
    df["date"] = pd.to_datetime(df["date"])
    df["date_int"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    return df


def check_consecutive_condition(values: list[bool], threshold: int) -> list[tuple[int, int]]:
    """
    找出连续满足条件的区间 (start_idx, end_idx)
    当连续 threshold 天满足条件时，从第 threshold 天开始标记为预警
    """
    intervals = []
    consecutive_count = 0
    start_idx = None

    for i, val in enumerate(values):
        if val:
            consecutive_count += 1
            if consecutive_count >= threshold and start_idx is None:
                start_idx = i
        else:
            if start_idx is not None:
                intervals.append((start_idx, i - 1))
                start_idx = None
            consecutive_count = 0

    if start_idx is not None:
        intervals.append((start_idx, len(values) - 1))

    return intervals


def compute_warning_intervals(df: pd.DataFrame) -> dict[str, list[list[int]]]:
    intervals_by_code: dict[str, list[list[int]]] = defaultdict(list)

    for instrument, group in df.groupby("instrument"):
        group = group.sort_values("date_int").reset_index(drop=True)
        dates = group["date_int"].tolist()
        closes = group["close"].tolist()
        market_caps = group["total_market_cap"].tolist()
        sectors = group["list_sector"].tolist()

        is_main_board = sectors[0] == LIST_SECTOR_MAIN_BOARD if sectors else False
        market_cap_threshold = MARKET_CAP_THRESHOLD_MAIN if is_main_board else MARKET_CAP_THRESHOLD_OTHER

        price_condition = [c < PRICE_THRESHOLD for c in closes]
        market_cap_condition = [mc < market_cap_threshold for mc in market_caps]

        combined_condition = [p or m for p, m in zip(price_condition, market_cap_condition)]

        interval_indices = check_consecutive_condition(combined_condition, CONSECUTIVE_DAYS)

        for start_idx, end_idx in interval_indices:
            intervals_by_code[instrument].append([dates[start_idx], dates[end_idx]])

    for intervals in intervals_by_code.values():
        intervals.sort(key=lambda x: x[0])

    return dict(intervals_by_code)


def merge_overlapping_intervals(intervals: list[list[int]]) -> list[list[int]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x[0])
    merged = [intervals[0]]
    for start, end in intervals[1:]:
        if start <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def main():
    print("Fetching data from bigquant...")
    df = fetch_data()
    print(f"  Loaded {len(df)} records, {df['instrument'].nunique()} instruments")

    print("Computing warning intervals...")
    intervals_by_code = compute_warning_intervals(df)

    for code in intervals_by_code:
        intervals_by_code[code] = merge_overlapping_intervals(intervals_by_code[code])

    print(f"  Found {len(intervals_by_code)} instruments with trading delist warning intervals")

    output = [{code: intervals_by_code[code]} for code in sorted(intervals_by_code)]
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"Output written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
