import json
from collections import defaultdict
from pathlib import Path

import dai
import pandas as pd


OUTPUT_PATH = Path(__file__).with_suffix(".json")

DIVIDEND_RATIO_THRESHOLD = 0.30
DIVIDEND_MIN_THRESHOLD = 5000 * 1e4
TRADING_DAYS_3Y = 750
LIST_SECTOR_MAIN_BOARD = 1


def fetch_dividend_amount() -> pd.DataFrame:
    """
    获取分红预案数据，计算分红金额 = 每股分红(税前) * 总股本
    数据源: cn_stock_dividend + cn_stock_capital
    """
    sql_dividend = """
    SELECT 
        date,
        instrument,
        cash_before_tax 
    FROM cn_stock_dividend
    WHERE cash_before_tax IS NOT NULL
    ORDER BY date, instrument
    """
    dividend_df = dai.query(sql_dividend, full_db_scan=True).df()
    dividend_df["date"] = pd.to_datetime(dividend_df["date"])
    
    sql_capital = """
    SELECT 
        publish_date AS date, 
        instrument,
        total_shares
    FROM cn_stock_capital 
    WHERE change_date = date AND total_shares IS NOT NULL
    ORDER BY date, instrument
    """
    capital_df = dai.query(sql_capital, full_db_scan=True).df()
    capital_df["date"] = pd.to_datetime(capital_df["date"])
    
    merged = pd.merge(dividend_df, capital_df, on=["date", "instrument"], how="outer")
    merged = merged.sort_values(["instrument", "date"])
    merged["cash_before_tax"] = merged.groupby("instrument")["cash_before_tax"].ffill()
    merged["total_shares"] = merged.groupby("instrument")["total_shares"].ffill()
    merged = merged.dropna(subset=["cash_before_tax", "total_shares"])
    merged["dividend_amount"] = merged["cash_before_tax"] * merged["total_shares"]
    
    return merged[["date", "instrument", "dividend_amount"]]


def fetch_avg_net_profit() -> pd.DataFrame:
    """
    获取近2年平均净利润
    数据源: cn_stock_financial_income_general_pit (年报数据)
    """
    sql = """
    SELECT 
        date,
        instrument,
        report_date,
        net_profit_to_parent_shareholders
    FROM cn_stock_financial_income_general_pit
    WHERE EXTRACT(MONTH FROM report_date) = 12
      AND net_profit_to_parent_shareholders IS NOT NULL
    ORDER BY instrument, date
    """
    df = dai.query(sql, full_db_scan=True).df()
    df["date"] = pd.to_datetime(df["date"])
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["report_year"] = df["report_date"].dt.year
    
    result_rows = []
    for instrument, group in df.groupby("instrument"):
        group = group.sort_values("date").drop_duplicates(subset=["report_year"], keep="first")
        group = group.sort_values("report_year").reset_index(drop=True)
        
        for i, row in group.iterrows():
            if i < 1:
                continue
            recent_2y = group.iloc[max(0, i-1):i+1]
            avg_profit = recent_2y["net_profit_to_parent_shareholders"].mean()
            result_rows.append({
                "date": row["date"],
                "instrument": instrument,
                "avg_net_profit": avg_profit,
            })
    
    return pd.DataFrame(result_rows)


def fetch_sector_data() -> dict[str, int]:
    sql = """
    SELECT instrument, list_sector
    FROM cn_stock_basic_info
    """
    df = dai.query(sql, full_db_scan=True).df()
    return {row["instrument"]: int(row["list_sector"]) 
            for _, row in df.iterrows() if pd.notna(row["list_sector"])}


def compute_warning_intervals(
    dividend_df: pd.DataFrame,
    net_profit_df: pd.DataFrame,
    sector_map: dict[str, int],
) -> dict[str, list[list[int]]]:
    """
    计算分红ST区间
    条件: avg_net_profit > 0 且 3年分红 < avg_net_profit*30% 且 3年分红 < 5000万
    仅主板适用
    """
    merged = pd.merge(dividend_df, net_profit_df, on=["date", "instrument"], how="outer")
    merged = merged.sort_values(["instrument", "date"])
    
    intervals_by_code: dict[str, list[list[int]]] = defaultdict(list)
    
    for instrument, group in merged.groupby("instrument"):
        sector = sector_map.get(instrument)
        if sector != LIST_SECTOR_MAIN_BOARD:
            continue
        
        group = group.sort_values("date").reset_index(drop=True)
        group["dividend_amount"] = group["dividend_amount"].ffill()
        group["avg_net_profit"] = group["avg_net_profit"].ffill()
        group["y3_dividend"] = group["dividend_amount"].rolling(window=TRADING_DAYS_3Y, min_periods=1).sum()
        
        in_warning = False
        warning_start = None
        
        for _, row in group.iterrows():
            avg_profit = row["avg_net_profit"]
            y3_div = row["y3_dividend"]
            
            if pd.isna(avg_profit) or pd.isna(y3_div):
                continue
            
            is_warning = (
                avg_profit > 0 and 
                y3_div < avg_profit * DIVIDEND_RATIO_THRESHOLD and 
                y3_div < DIVIDEND_MIN_THRESHOLD
            )
            
            if is_warning and not in_warning:
                warning_start = row["date"]
                in_warning = True
            elif not is_warning and in_warning:
                start_int = int(warning_start.strftime("%Y%m%d"))
                end_int = int((row["date"] - pd.Timedelta(days=1)).strftime("%Y%m%d"))
                if end_int >= start_int:
                    intervals_by_code[instrument].append([start_int, end_int])
                in_warning = False
        
        if in_warning and warning_start is not None:
            start_int = int(warning_start.strftime("%Y%m%d"))
            end_int = int((group["date"].iloc[-1] + pd.Timedelta(days=365)).strftime("%Y%m%d"))
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
    print("Fetching dividend data (cn_stock_dividend + cn_stock_capital)...")
    dividend_df = fetch_dividend_amount()
    print(f"  Loaded {len(dividend_df)} dividend records, {dividend_df['instrument'].nunique()} instruments")
    
    print("Fetching net profit data (cn_stock_financial_ly_shift)...")
    net_profit_df = fetch_avg_net_profit()
    print(f"  Loaded {len(net_profit_df)} net profit records, {net_profit_df['instrument'].nunique()} instruments")
    
    print("Fetching sector data...")
    sector_map = fetch_sector_data()
    print(f"  Loaded {len(sector_map)} sector records")
    
    print("Computing warning intervals...")
    intervals_by_code = compute_warning_intervals(dividend_df, net_profit_df, sector_map)
    
    for code in intervals_by_code:
        intervals_by_code[code] = merge_overlapping_intervals(intervals_by_code[code])
    
    total_intervals = sum(len(v) for v in intervals_by_code.values())
    print(f"  Found {len(intervals_by_code)} instruments with {total_intervals} dividend ST intervals")
    
    output = [{code: intervals_by_code[code]} for code in sorted(intervals_by_code)]
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"Output written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
