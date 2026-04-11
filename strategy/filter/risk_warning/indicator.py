import json
from collections import defaultdict
from pathlib import Path

import dai
import pandas as pd


OUTPUT_PATH = Path(__file__).with_suffix(".json")


def fetch_risk_warning_dates() -> pd.DataFrame:
    """
    获取所有风险警示记录
    返回: DataFrame with columns [instrument, date_int]
    """
    sql = """
    SELECT date, instrument
    FROM cn_stock_status
    WHERE is_risk_warning = 1
    """
    df = dai.query(sql, full_db_scan=True).df()
    df["date"] = pd.to_datetime(df["date"])
    df["date_int"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    return df[["instrument", "date_int"]].drop_duplicates()


def dates_to_intervals(dates: list[int]) -> list[list[int]]:
    """
    将日期列表转换为连续区间列表
    输入: [20210101, 20210104, 20210105, 20210106, 20210201]
    输出: [[20210101, 20210101], [20210104, 20210106], [20210201, 20210201]]
    """
    if not dates:
        return []
    
    dates = sorted(dates)
    intervals = []
    start = dates[0]
    prev = dates[0]
    
    for d in dates[1:]:
        if d - prev > 7:
            intervals.append([start, prev])
            start = d
        prev = d
    
    intervals.append([start, prev])
    return intervals


def build_intervals_by_instrument(df: pd.DataFrame) -> dict[str, list[list[int]]]:
    """
    按instrument分组，生成interval区间
    """
    intervals_by_code = defaultdict(list)
    
    for instrument, group in df.groupby("instrument"):
        dates = group["date_int"].tolist()
        intervals = dates_to_intervals(dates)
        if intervals:
            intervals_by_code[instrument] = intervals
    
    return dict(intervals_by_code)


def main():
    print("Fetching risk warning data from bigquant...")
    df = fetch_risk_warning_dates()
    print(f"  Loaded {len(df)} risk warning records")
    
    print("Building intervals...")
    intervals_by_code = build_intervals_by_instrument(df)
    print(f"  Found {len(intervals_by_code)} instruments with risk_warning intervals")
    
    output = [{code: intervals_by_code[code]} for code in sorted(intervals_by_code)]
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"Output written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
