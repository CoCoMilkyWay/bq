from bigmodule import M, I
import dai
import pandas as pd

'''
## 策略配置
- **股票池** (`cn_stock_prefactors_community`)
  - 按 `total_market_cap` 升序取前500只
  - 排除 ST、停牌、北交所
  - 每日重算
- **过滤因子**
  - 股票池排除过滤因子=1的标的
  - 每日重算
- **排序因子**
  - 过滤后的股票池, 根据排序因子加权计算得分, 得到最终当日持仓标的
  - TODO: 暂时没有排序因子, 持有所有股票池内标的
- **持仓**
  - 等权分配，每日调仓

过滤因子:
**预期连续两年亏损**:
    `forecast_2_year_loss := 前年亏损 AND 去年预亏 AND 年报未发 AND 年报截至前(4月底)`
    - 前年亏损 = `last_parent_net < 0`
    - 去年预亏 = `type ∈ {'首亏', '续亏'}` (年报: `end_date[4:6]=='12'`)
    - 年报未发 = `date < disclosure.actual_date`
    - 年报截至 = `disclosure.actual_date ?? (end_date.year+1, 4, monthend) 次年4月月末`

排序因子: 无

代码编写原则:
1. 回测vector mode应该和实盘incremental mode实现一致, 尽量共享代码和逻辑
2. 每个因子的实现应该尽量独立定义在代码最前, 不要和后面的框架耦合
'''



def m5_initialize_bigquant_run(context):
    from bigtrader.finance.commission import PerOrder
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))


def m5_before_trading_start_bigquant_run(context, data):
    pass


def m5_handle_tick_bigquant_run(context, tick):
    pass


def m5_handle_data_bigquant_run(context, data):
    today_df = context.data[context.data["date"] == data.current_dt.strftime("%Y-%m-%d")]
    target_instruments = set(today_df["instrument"])
    holding_instruments = set(context.get_account_positions().keys())

    for instrument in holding_instruments - target_instruments:
        context.order_target_percent(instrument, 0)

    for i, x in today_df.iterrows():
        position = 0.0 if pd.isnull(x.position) else float(x.position)
        context.order_target_percent(x.instrument, position)


def m5_handle_trade_bigquant_run(context, trade):
    pass


def m5_handle_order_bigquant_run(context, order):
    pass


def m5_after_trading_bigquant_run(context, data):
    pass


# ========== 数据准备 ==========
# 预期ST因子：去年年报亏损 AND 当年预亏公告（年报披露前有效）

# ==================== 预计算数据（只需运行一次）====================
# 预计算年报披露日期和 net_profit_ly，避免在策略 SQL 中 JOIN 大表

# 1. 年报披露日期（通过 net_profit_ly 变化推断，每年取第一次变化日期）
ANNUAL_REPORT_SQL = """
SELECT DISTINCT ON (instrument, YEAR(date))
    date AS publish_date,
    instrument,
    YEAR(date) AS publish_year
FROM (
    SELECT 
        date,
        instrument,
        net_profit_ly,
        LAG(net_profit_ly) OVER (PARTITION BY instrument ORDER BY date) AS prev_profit
    FROM cn_stock_factors_financial_items
)
WHERE net_profit_ly IS DISTINCT FROM prev_profit
  AND net_profit_ly IS NOT NULL
  AND MONTH(date) IN (1,2,3,4)
ORDER BY instrument, YEAR(date), date
"""

print("预计算年报披露日期...")
annual_report_df = dai.query(ANNUAL_REPORT_SQL, filters={"date": ["2015-01-01", "2030-01-01"]}).df()
print(f"年报披露记录数：{len(annual_report_df)}")

# 2. 预计算 net_profit_ly（只提取需要的字段，大幅减少内存）
NET_PROFIT_SQL = """
SELECT date, instrument, net_profit_ly
FROM cn_stock_factors_financial_items
WHERE net_profit_ly IS NOT NULL
"""

print("预计算 net_profit_ly...")
net_profit_df = dai.query(NET_PROFIT_SQL, filters={"date": ["2015-01-01", "2030-01-01"]}).df()
print(f"net_profit_ly 记录数：{len(net_profit_df)}")

# ==================== 指标定义 ====================

# 预期ST指标SQL
# - annual_report_publish: 预计算的年报披露日期（通过 bind_relations 引用）
# - net_profit_ly: PIT数据，当日可知的最新年报利润，无未来信息
# - profit_estimate: 业绩预告公告日期，无未来信息
INDICATOR_EXPECTED_ST = """
-- 年报披露日期（预计算数据）
annual_report_publish AS (
    SELECT publish_date, instrument, publish_year
    FROM annual_report_ref
),

-- net_profit_ly（预计算数据）
net_profit_data AS (
    SELECT date, instrument, net_profit_ly
    FROM net_profit_ref
),

-- 业绩预告（只取年报预亏，end_date 月份=12，fore_profit < 0）
profit_estimate AS (
    SELECT
        date AS announce_date,
        CASE WHEN CAST(instrument AS INT) >= 600000
             THEN LPAD(CAST(CAST(instrument AS INT) AS VARCHAR), 6, '0') || '.SH'
             ELSE LPAD(CAST(CAST(instrument AS INT) AS VARCHAR), 6, '0') || '.SZ'
        END AS instrument,
        YEAR(end_date) AS fore_year
    FROM cn_stock_profit_estimate
    WHERE MONTH(end_date) = 12 AND fore_profit < 0
),

-- 计算预期ST指标
-- 核心逻辑：使用当日可知的 net_profit_ly（PIT数据，无未来信息）
expected_st AS (
    SELECT
        a.date,
        a.instrument,
        -- 当日可知的最新年报利润（PIT数据，在年报披露前是前年的值）
        b.net_profit_ly AS net_profit_ly_prev,
        -- 预告年份应为去年
        YEAR(a.date) - 1 AS target_fore_year,
        -- LAST 按 (instrument, year) 分区，扫描当年发布的预告
        LAST(d.fore_year IGNORE NULLS) OVER (
            PARTITION BY a.instrument, YEAR(a.date)
            ORDER BY a.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS latest_fore_year,
        -- 当年年报披露日期
        e.publish_date AS annual_report_date
    FROM cn_stock_prefactors_community a
    LEFT JOIN net_profit_data b USING (date, instrument)
    LEFT JOIN profit_estimate d ON a.instrument = d.instrument AND a.date = d.announce_date
    LEFT JOIN annual_report_publish e ON a.instrument = e.instrument AND YEAR(a.date) = e.publish_year
),

-- 预期ST = 前年年报亏损 AND 当年预亏公告（年报披露前有效）
with_expected_st AS (
    SELECT
        date,
        instrument,
        net_profit_ly_prev,
        target_fore_year,
        latest_fore_year,
        annual_report_date,
        CASE WHEN
            -- 条件1：前年年报亏损（net_profit_ly 是当日可知的最新年报）
            net_profit_ly_prev < 0
            -- 条件2：当年预亏公告
            AND latest_fore_year = target_fore_year
            -- 条件3：当年年报尚未披露（缺失则用4月30日作为默认截止）
            AND date < COALESCE(annual_report_date, MAKE_DATE(YEAR(date), 5, 1))
        THEN 1 ELSE 0 END AS is_expected_st
    FROM expected_st
),
"""

# ==================== 策略SQL ====================

stock_sql = f"""
WITH 
-- 股票池：市值最小500只（非ST、非停牌、非北交所）
small_cap_500 AS (
    SELECT date, instrument, total_market_cap
    FROM cn_stock_prefactors_community
    WHERE st_status = 0 AND suspended = 0 AND is_bz50 = 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_market_cap ASC) <= 500
),

{INDICATOR_EXPECTED_ST}

-- 筛选：股票池 + 指标=1
filtered AS (
    SELECT 
        a.date,
        a.instrument,
        a.total_market_cap,
        b.net_profit_ly_prev,
        b.latest_fore_year,
        b.annual_report_date,
        b.is_expected_st
    FROM small_cap_500 a
    JOIN with_expected_st b USING (date, instrument)
    WHERE b.is_expected_st = 1
)

SELECT
    date,
    instrument,
    total_market_cap,
    net_profit_ly_prev,
    latest_fore_year,
    annual_report_date,
    is_expected_st,
    -total_market_cap AS score,
    1.0 / c_sum(1) AS position
FROM filtered
ORDER BY date, instrument
"""

print("正在查询数据...")
filtered_df = dai.query(
    stock_sql, 
    filters={"date": ["2020-01-01", "2026-12-31"]},
    bind_relations={
        "annual_report_ref": annual_report_df,
        "net_profit_ref": net_profit_df
    }
).df()
print(f"满足预期ST条件的记录数：{len(filtered_df)}")
print(f"每日平均持仓数量：{filtered_df.groupby('date')['instrument'].count().mean():.1f}")

stock_data_ds = dai.DataSource.write_bdb(filtered_df)

# ========== 回测 ==========
start_date = '2021-01-01'
end_date = '2026-04-07'

m5 = M.bigtrader.v30(
    data=stock_data_ds,
    start_date=start_date,
    end_date=end_date,
    initialize=m5_initialize_bigquant_run,
    before_trading_start=m5_before_trading_start_bigquant_run,
    handle_tick=m5_handle_tick_bigquant_run,
    handle_data=m5_handle_data_bigquant_run,
    handle_trade=m5_handle_trade_bigquant_run,
    handle_order=m5_handle_order_bigquant_run,
    after_trading=m5_after_trading_bigquant_run,
    capital_base=1000000,
    frequency="daily",
    product_type="股票",
    rebalance_period_type="交易日",
    rebalance_period_days="1",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="标准模式",
    before_start_days=0,
    volume_limit=1,
    order_price_field_buy="open",
    order_price_field_sell="open",
    benchmark="沪深300指数",
    plot_charts=True,
    debug=False,
    backtest_only=False,
    m_name="m5"
)