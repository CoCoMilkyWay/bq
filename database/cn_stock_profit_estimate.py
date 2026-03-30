import dai
import pandas as pd
import re
from datetime import datetime

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 200)
pd.set_option("display.max_colwidth", 200)
pd.set_option("display.expand_frame_repr", False)

START = "2024-01-01"
END = datetime.today().strftime("%Y-%m-%d")

FORE_TYPE_NAME = {
    1: "业绩大幅上升", 2: "业绩大幅下降", 3: "上升不超过50%", 4: "下降不超过50%",
    5: "预盈", 6: "预亏", 7: "扭亏", 8: "减亏", 9: "持平",
    10: "预警", 11: "其它", 12: "撤消预计", 13: "不确定", 14: "预增", 15: "预减",
}

def normalize_cn_stock_code(x):
    if pd.isna(x):
        return None
    s = str(x).strip().upper()
    if re.fullmatch(r"\d{6}\.(SZ|SH|BJ)", s):
        return s
    m = re.search(r"\d+", s)
    if not m:
        return None
    code = m.group(0).zfill(6)
    if code.startswith(("60", "68")):
        return f"{code}.SH"
    if code.startswith(("00", "30", "02")):
        return f"{code}.SZ"
    if code.startswith(("83", "87", "88")):
        return f"{code}.BJ"
    return f"{code}.SH" if code.startswith("6") else f"{code}.SZ"

def assign_bin_by_rank(rank: int, n: int) -> str:
    if rank <= 500:   return "top500"
    if rank <= 1000:  return "top1000"
    if rank <= 2000:  return "top2000"
    if rank > n - 100:  return "bot100"
    if rank > n - 500:  return "bot500"
    if rank > n - 1000: return "bot1000"
    return "mid"

# 1) 取预告数据 + 标准化证券代码
est = dai.query(f"SELECT date, instrument, fore_type FROM cn_stock_profit_estimate WHERE date BETWEEN '{START}' AND '{END}'").df()
assert not est.empty, "cn_stock_profit_estimate 为空"

est["date"] = pd.to_datetime(est["date"], errors="coerce")
est = est.dropna(subset=["date", "instrument"])
est["instrument_raw"] = est["instrument"]
est["instrument"] = est["instrument"].apply(normalize_cn_stock_code)
est = est.dropna(subset=["instrument"])
est["month"] = est["date"].dt.to_period("M").dt.to_timestamp()
est["fore_type_name"] = est["fore_type"].map(FORE_TYPE_NAME).fillna("unknown_type")

# 2) 取估值市值：每月每只股票当月最后一个交易日
val = dai.query(f"SELECT date, instrument, total_market_cap FROM cn_stock_valuation WHERE date BETWEEN '{START}' AND '{END}'").df()
assert not val.empty, "cn_stock_valuation 为空"

val["date"] = pd.to_datetime(val["date"], errors="coerce")
val = val.dropna(subset=["date", "instrument", "total_market_cap"])
val["instrument"] = val["instrument"].astype(str).str.upper()
val["month"] = val["date"].dt.to_period("M").dt.to_timestamp()

# (month, instrument) 取该月最后一个有市值数据的交易日记录
val = val.sort_values(["month", "instrument", "date"]).groupby(["month", "instrument"], as_index=False).tail(1)
val["n_in_month"] = val.groupby("month")["instrument"].transform("nunique")
val["mcap_rank"] = val.groupby("month")["total_market_cap"].rank(method="first", ascending=False).astype(int)
val["bin"] = val.apply(lambda x: assign_bin_by_rank(int(x["mcap_rank"]), int(x["n_in_month"])), axis=1)
bin_map = val[["month", "instrument", "bin"]].drop_duplicates()

# 3) merge：预告匹配当月月末市值bin
est2 = est.merge(bin_map, on=["month", "instrument"], how="left", indicator=True)
est2["bin"] = est2["bin"].fillna("unknown")

print("\n=== merge 诊断 ===")
print("est instrument sample:", est["instrument_raw"].head().tolist(), "->", est["instrument"].head().tolist())
print(est2["_merge"].value_counts())
print("match rate:", (est2["_merge"] == "both").mean())

# 4) 每月各 bin 的预告行数
cols = ["top500", "top1000", "top2000", "mid", "bot1000", "bot500", "bot100", "unknown"]
cnt = est2.groupby(["month", "bin"]).size().unstack("bin", fill_value=0).reindex(columns=cols, fill_value=0)
print(f"\n按月末市值分层后的当月预告统计（行数，不去重）: {START} ~ {END}")
print(cnt)

# 5) 事件类型统计

print("\n=== 事件类型按bin：month x bin x fore_type_name ===")
pivot = est2.groupby(["month", "bin", "fore_type_name"]).size().unstack("fore_type_name", fill_value=0)
print(pivot.to_string(col_space=10))
