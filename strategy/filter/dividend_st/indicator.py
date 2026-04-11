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


def fetch_financial_data() -> pd.DataFrame:
    """
    获取年报数据：归母净利润 + 支付的股利现金
    两个 PIT 表 JOIN，确保单位一致（都是总金额，单位：元）
    """
    sql = """
    SELECT 
        i.date,
        i.instrument,
        i.report_date,
        i.net_profit_to_parent_shareholders,
        c.cash_paid_for_dividends_profits_interests
    FROM cn_stock_financial_income_general_pit i
    LEFT JOIN cn_stock_financial_cashflow_general_pit c
        ON i.date = c.date 
        AND i.instrument = c.instrument 
        AND i.report_date = c.report_date
    WHERE EXTRACT(MONTH FROM i.report_date) = 12
      AND i.net_profit_to_parent_shareholders IS NOT NULL
    """
    df = dai.query(sql, full_db_scan=True).df()
    df["date"] = pd.to_datetime(df["date"])
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["report_year"] = df["report_date"].dt.year
    df["dividend"] = df["cash_paid_for_dividends_profits_interests"].fillna(0).clip(lower=0)
    return df


def fetch_sector_data() -> dict[str, int]:
    sql = """
    SELECT instrument, list_sector
    FROM cn_stock_basic_info
    """
    df = dai.query(sql, full_db_scan=True).df()
    return {row["instrument"]: int(row["list_sector"]) 
            for _, row in df.iterrows() if pd.notna(row["list_sector"])}


def compute_warning_intervals(
    financial_df: pd.DataFrame,
    sector_map: dict[str, int],
) -> dict[str, list[list[int]]]:
    
    intervals_by_code: dict[str, list[list[int]]] = defaultdict(list)
    
    for instrument, group in financial_df.groupby("instrument"):
        sector = sector_map.get(instrument)
        if sector != LIST_SECTOR_MAIN_BOARD:
            continue
        
        group = group.sort_values("date")
        group = group.drop_duplicates(subset=["report_year"], keep="first")
        group = group.sort_values("report_year").reset_index(drop=True)
        
        if len(group) < YEARS_LOOKBACK:
            continue
        
        for i in range(YEARS_LOOKBACK - 1, len(group)):
            rows = [group.iloc[j] for j in range(i - YEARS_LOOKBACK + 1, i + 1)]
            
            total_dividend = sum(r["dividend"] for r in rows)
            total_profit = sum(r["net_profit_to_parent_shareholders"] for r in rows)
            avg_profit = total_profit / YEARS_LOOKBACK
            
            if avg_profit <= 0:
                continue
            
            threshold = avg_profit * DIVIDEND_RATIO_THRESHOLD
            
            if total_dividend < threshold and total_dividend < DIVIDEND_MIN_THRESHOLD:
                current_row = rows[-1]
                start_date = current_row["date"]
                
                if i + 1 < len(group):
                    end_date = group.iloc[i + 1]["date"] - pd.Timedelta(days=1)
                else:
                    end_date = start_date + pd.Timedelta(days=365)
                
                if end_date < start_date:
                    continue
                
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
    print("Fetching financial data from bigquant...")
    financial_df = fetch_financial_data()
    print(f"  Loaded {len(financial_df)} records, {financial_df['instrument'].nunique()} instruments")
    
    print("Fetching sector data from bigquant...")
    sector_map = fetch_sector_data()
    print(f"  Loaded {len(sector_map)} sector records")
    
    print("Computing warning intervals...")
    intervals_by_code = compute_warning_intervals(financial_df, sector_map)
    
    for code in intervals_by_code:
        intervals_by_code[code] = merge_overlapping_intervals(intervals_by_code[code])
    
    total_intervals = sum(len(v) for v in intervals_by_code.values())
    print(f"  Found {len(intervals_by_code)} instruments with {total_intervals} dividend ST intervals")
    
    output = [{code: intervals_by_code[code]} for code in sorted(intervals_by_code)]
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"Output written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
