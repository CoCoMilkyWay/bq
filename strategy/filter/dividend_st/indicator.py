import json
from collections import defaultdict
from pathlib import Path

import dai
import pandas as pd


OUTPUT_PATH = Path(__file__).with_suffix(".json")

DIVIDEND_RATIO_THRESHOLD = 0.30
DIVIDEND_MIN_THRESHOLD = 5000 * 1e4
YEARS_LOOKBACK = 3
LIST_SECTOR_MAIN_BOARD = 1


def fetch_dividend_data() -> pd.DataFrame:
    """
    获取分红数据，按报告年度汇总
    cash_before_tax 是税前现金分红（每股，需要乘以股数得到总额）
    这里直接用 cash_before_tax 作为分红金额的代理
    """
    sql = """
    SELECT 
        instrument,
        EXTRACT(YEAR FROM report_date) AS report_year,
        publish_date,
        cash_before_tax
    FROM cn_stock_dividend
    WHERE cash_before_tax IS NOT NULL AND cash_before_tax > 0
    """
    df = dai.query(sql, full_db_scan=True).df()
    df["publish_date"] = pd.to_datetime(df["publish_date"])
    return df


def fetch_net_profit_data() -> pd.DataFrame:
    """
    获取年报归母净利润数据 (使用 ly 表示上一年度完整年报)
    """
    sql = """
    SELECT 
        date,
        instrument,
        report_date,
        net_profit_to_parent_shareholders_ly
    FROM cn_stock_financial_ly_shift
    WHERE shift = 0 
      AND EXTRACT(MONTH FROM report_date) = 12
      AND net_profit_to_parent_shareholders_ly IS NOT NULL
    """
    df = dai.query(sql, full_db_scan=True).df()
    df["date"] = pd.to_datetime(df["date"])
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["report_year"] = df["report_date"].dt.year
    return df


def fetch_sector_data() -> dict[str, int]:
    sql = """
    SELECT instrument, list_sector
    FROM cn_stock_basic_info
    """
    df = dai.query(sql, full_db_scan=True).df()
    return {row["instrument"]: int(row["list_sector"]) 
            for _, row in df.iterrows() if pd.notna(row["list_sector"])}


def aggregate_dividend_by_year(dividend_df: pd.DataFrame) -> dict[tuple[str, int], float]:
    """
    按 (instrument, report_year) 汇总分红
    """
    grouped = dividend_df.groupby(["instrument", "report_year"])["cash_before_tax"].sum()
    return grouped.to_dict()


def compute_warning_intervals(
    dividend_by_year: dict[tuple[str, int], float],
    profit_df: pd.DataFrame,
    sector_map: dict[str, int],
) -> dict[str, list[list[int]]]:
    
    intervals_by_code: dict[str, list[list[int]]] = defaultdict(list)
    
    profit_df = profit_df.sort_values(["instrument", "date"])
    
    for instrument, group in profit_df.groupby("instrument"):
        sector = sector_map.get(instrument)
        if sector != LIST_SECTOR_MAIN_BOARD:
            continue
        
        group = group.drop_duplicates(subset=["report_year"], keep="first")
        group = group.sort_values("report_year").reset_index(drop=True)
        
        if len(group) < YEARS_LOOKBACK:
            continue
        
        for i in range(YEARS_LOOKBACK - 1, len(group)):
            years = [group.iloc[j]["report_year"] for j in range(i - YEARS_LOOKBACK + 1, i + 1)]
            profits = [group.iloc[j]["net_profit_to_parent_shareholders_ly"] for j in range(i - YEARS_LOOKBACK + 1, i + 1)]
            
            total_dividend = sum(dividend_by_year.get((instrument, y), 0) for y in years)
            avg_profit = sum(profits) / YEARS_LOOKBACK
            
            if avg_profit <= 0:
                continue
            
            threshold = avg_profit * DIVIDEND_RATIO_THRESHOLD
            
            if total_dividend < threshold and total_dividend < DIVIDEND_MIN_THRESHOLD:
                current_row = group.iloc[i]
                start_date = current_row["date"]
                
                if i + 1 < len(group):
                    end_date = group.iloc[i + 1]["date"] - pd.Timedelta(days=1)
                else:
                    end_date = start_date + pd.Timedelta(days=365)
                
                start_int = int(start_date.strftime("%Y%m%d"))
                end_int = int(end_date.strftime("%Y%m%d"))
                
                intervals_by_code[instrument].append([start_int, end_int])
    
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
    print("Fetching dividend data from bigquant...")
    dividend_df = fetch_dividend_data()
    print(f"  Loaded {len(dividend_df)} dividend records")
    
    print("Fetching net profit data from bigquant...")
    profit_df = fetch_net_profit_data()
    print(f"  Loaded {len(profit_df)} profit records")
    
    print("Fetching sector data from bigquant...")
    sector_map = fetch_sector_data()
    print(f"  Loaded {len(sector_map)} sector records")
    
    print("Aggregating dividend by year...")
    dividend_by_year = aggregate_dividend_by_year(dividend_df)
    print(f"  Aggregated {len(dividend_by_year)} (instrument, year) pairs")
    
    print("Computing warning intervals...")
    intervals_by_code = compute_warning_intervals(dividend_by_year, profit_df, sector_map)
    
    for code in intervals_by_code:
        intervals_by_code[code] = merge_overlapping_intervals(intervals_by_code[code])
    
    print(f"  Found {len(intervals_by_code)} instruments with dividend ST warning intervals")
    
    output = [{code: intervals_by_code[code]} for code in sorted(intervals_by_code)]
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"Output written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
