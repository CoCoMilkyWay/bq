"""
生成次新股过滤因子
过滤上市不满60天的标的
"""

import json
from pathlib import Path

import pandas as pd
import dai

MIN_LIST_DAYS = 60


def main():
    print("获取所有股票上市日期...")
    sql = "SELECT instrument, list_date FROM cn_stock_basic_info WHERE list_date IS NOT NULL"
    df = dai.query(sql).df()
    print(f"股票数量: {len(df)}")
    
    df["list_date"] = pd.to_datetime(df["list_date"])
    
    results = []
    for _, row in df.iterrows():
        inst = row["instrument"]
        list_date = row["list_date"]
        
        start_int = int(list_date.strftime("%Y%m%d"))
        end_date = list_date + pd.Timedelta(days=MIN_LIST_DAYS - 1)
        end_int = int(end_date.strftime("%Y%m%d"))
        
        results.append({inst: [[start_int, end_int]]})
    
    output_path = Path(__file__).parent / "indicator.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"已保存到 {output_path}")
    print(f"过滤区间数: {len(results)}")


if __name__ == "__main__":
    main()
