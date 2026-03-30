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

# 1) 取盈利预测数据（只保留 forecast_np_fy1 非空的记录）
fc = dai.query(f"SELECT date, instrument, forecast_np_fy1 FROM cn_stock_financial_forecast_consensus_rolling WHERE date BETWEEN '{START}' AND '{END}'").df()
assert not fc.empty, "cn_stock_financial_forecast_consensus_rolling 为空"

fc["date"] = pd.to_datetime(fc["date"], errors="coerce")
fc = fc.dropna(subset=["date", "instrument", "forecast_np_fy1"])  # 关键：预测值非空才算覆盖
fc["instrument"] = fc["instrument"].apply(normalize_cn_stock_code)
fc = fc.dropna(subset=["instrument"])
fc["month"] = fc["date"].dt.to_period("M").dt.to_timestamp()

# 每月每只股票只算一次覆盖
fc_monthly = fc.groupby(["month", "instrument"]).size().reset_index(name="fc_count")

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

# 3) 统计每月各 bin 的股票总数
bin_total = val.groupby(["month", "bin"]).size().reset_index(name="total")

# 4) merge：预测数据匹配市值 bin
fc_with_bin = fc_monthly.merge(val[["month", "instrument", "bin"]], on=["month", "instrument"], how="inner")
fc_covered = fc_with_bin.groupby(["month", "bin"]).size().reset_index(name="covered")

# 5) 计算覆盖率
coverage = bin_total.merge(fc_covered, on=["month", "bin"], how="left")
coverage["covered"] = coverage["covered"].fillna(0).astype(int)
coverage["rate"] = (coverage["covered"] / coverage["total"] * 100).round(1)

# 6) 输出：覆盖股票数
cols = ["top500", "top1000", "top2000", "mid", "bot1000", "bot500", "bot100"]
pivot_covered = coverage.pivot(index="month", columns="bin", values="covered").reindex(columns=cols, fill_value=0)
print(f"\n=== 盈利预测覆盖股票数（按月末市值分层）: {START} ~ {END} ===")
print(pivot_covered.to_string(col_space=10))

# 7) 输出：覆盖率 %
pivot_rate = coverage.pivot(index="month", columns="bin", values="rate").reindex(columns=cols, fill_value=0)
print(f"\n=== 盈利预测覆盖率 %（按月末市值分层）: {START} ~ {END} ===")
print(pivot_rate.to_string(col_space=10))

# 8) 输出：各 bin 总股票数（供参考）
pivot_total = coverage.pivot(index="month", columns="bin", values="total").reindex(columns=cols, fill_value=0)
print(f"\n=== 各 bin 股票总数: {START} ~ {END} ===")
print(pivot_total.to_string(col_space=10))
