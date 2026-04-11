import json
from pathlib import Path

import dai
import pandas as pd


OUTPUT_PATH = Path(__file__).with_suffix(".json")

LIST_DAYS_THRESHOLD = 60


def fetch_list_dates() -> pd.DataFrame:
    """
    获取每个股票的上市日期
    数据源: cn_stock_prefactors
    """
    sql = """
    SELECT DISTINCT
        instrument,
        list_date
    FROM cn_stock_prefactors
    WHERE list_date IS NOT NULL
    """
    df = dai.query(sql, full_db_scan=True).df()
    df["list_date"] = pd.to_datetime(df["list_date"])
    return df


def compute_warning_intervals(list_dates_df: pd.DataFrame) -> dict[str, list[list[int]]]:
    """
    计算上市天数不足的区间
    区间: [list_date, list_date + LIST_DAYS_THRESHOLD]
    """
    intervals_by_code: dict[str, list[list[int]]] = {}
    
    for _, row in list_dates_df.iterrows():
        instrument = row["instrument"]
        list_date = row["list_date"]
        
        if pd.isna(list_date):
            continue
        
        start_date = list_date
        end_date = list_date + pd.Timedelta(days=LIST_DAYS_THRESHOLD)
        
        start_int = int(start_date.strftime("%Y%m%d"))
        end_int = int(end_date.strftime("%Y%m%d"))
        
        intervals_by_code[instrument] = [[start_int, end_int]]
    
    return intervals_by_code


def main():
    print("Fetching list dates from cn_stock_prefactors...")
    list_dates_df = fetch_list_dates()
    print(f"  Loaded {len(list_dates_df)} instruments")
    
    print("Computing warning intervals...")
    intervals_by_code = compute_warning_intervals(list_dates_df)
    
    total_intervals = sum(len(v) for v in intervals_by_code.values())
    print(f"  Found {len(intervals_by_code)} instruments with {total_intervals} list_days intervals")
    
    output = [{code: intervals_by_code[code]} for code in sorted(intervals_by_code)]
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"Output written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
