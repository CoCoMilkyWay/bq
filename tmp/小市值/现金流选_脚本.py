from bigquant import bigtrader, dai
import pandas as pd

def initialize(context: bigtrader.IContext):
    # 手续费设置
    context.set_commission(bigtrader.PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))

    context.logger.info("开始计算选股与排名因子...")

    # 策略核心参数
    context.top_hold = 5          # 等权买入前5名
    context.keep_threshold = 9    # 掉出前9名才卖

    # 为了计算两个1年滞后的财务指标，需要向前多取大约1年数据
    start_date_with_lag = context.add_trading_days(context.start_date, -260)

    sql = """
    WITH base AS (
        SELECT
            date,
            instrument,
            close,
            momentum_5,
            total_market_cap,
            float_market_cap,
            net_cffoa_ttm,          -- 经营活动产生的现金流量净额TTM
            net_profit_ttm,         -- 净利润TTM
            st_status,
            list_sector,
            price_limit_status,
            sw_level1_name,         -- 申万一级行业名称（2021版）
            -- 约1年前的净利润和经营现金流
            m_lag(net_profit_ttm, 252)  AS net_profit_ttm_1y_ago,
            m_lag(net_cffoa_ttm,  252)  AS net_cffoa_ttm_1y_ago
        FROM cn_stock_prefactors
    ),
    filtered AS (
        SELECT
            *,
            CASE
                WHEN net_cffoa_ttm_1y_ago IS NOT NULL
                     AND net_cffoa_ttm_1y_ago != 0
                THEN net_cffoa_ttm / net_cffoa_ttm_1y_ago - 1
                ELSE NULL
            END AS growth_cffoa_12m
        FROM base
        WHERE
            -- 板块过滤：去掉科创板(3)、创业板(2)、北交所(4)
            list_sector NOT IN (2, 3, 4)
            -- 排除ST
            AND st_status = 0
            -- 连续两年不亏损：两次TTM净利润都>0
            AND net_profit_ttm > 0
            AND net_profit_ttm_1y_ago > 0
            -- 去掉5日涨幅>50%的股票
            AND momentum_5 <= 0.5
            -- 只在允许交易的月份生成选股信号：2,3,5,6,7,8,9,10,11
            AND month(date) IN (2, 3, 5, 6, 7, 8, 9, 10, 11)
            -- 去掉申万2021一级行业：农林牧渔、房地产、钢铁、银行
            AND sw_level1_name NOT IN ('农林牧渔', '房地产', '钢铁', '银行')
    ),
    ranked AS (
        SELECT
            date,
            instrument,
            close,
            price_limit_status,
            total_market_cap,
            float_market_cap,
            growth_cffoa_12m,
            -- 经营现金流增速从小到大（越小越好，权重1）
            c_rank(growth_cffoa_12m, ascending:=true) AS rank_cffoa,
            -- 总市值从小到大（越小越好，权重7）
            c_rank(total_market_cap, ascending:=true) AS rank_total_mkt,
            -- 流通市值从小到大（越小越好，权重7）
            c_rank(float_market_cap, ascending:=true) AS rank_float_mkt,
            -- 收盘价从小到大（越小越好，权重1）
            c_rank(close, ascending:=true) AS rank_close
        FROM filtered
    ),
    scored AS (
        SELECT
            date,
            instrument,
            close,
            price_limit_status,
            (1*rank_cffoa + 7*rank_total_mkt + 7*rank_float_mkt + 1*rank_close) AS score
        FROM ranked
    ),
    ranked_score AS (
        SELECT
            *,
            c_rank(score, ascending:=true) AS rank_score
        FROM scored
    )
    SELECT
        date,
        instrument,
        close,
        price_limit_status,
        score,
        rank_score
    FROM ranked_score
    ORDER BY date, rank_score, instrument
    """

    df = dai.query(
        sql,
        filters={"date": [start_date_with_lag, context.end_date]}
    ).df()

    # 只保留正式回测区间内的数据
    df = df[df["date"] >= context.start_date].copy()
    context.logger.info(f"选股打分数据量: {len(df)}")

    # 存入 context，handle_data 使用
    context.data = df

def handle_data(context: bigtrader.IContext, data: bigtrader.IBarData):
    """
    交易逻辑：
    - 每个允许交易的月份（2,3,5,6,7,8,9,10,11）的每个交易日调仓（按收盘价近似14:55）
    - 等权买入当天排名前5名且“未跌停”的股票
    - 卖出条件：
        * 持仓股票当天不在前9名（rank_score > 9）
        * 且当天不是涨停 (price_limit_status != 3)
      => 清仓
    - 不卖条件：
        * 当天涨停 (price_limit_status = 3)，无论排名如何都不卖
        * 当天仍在前9名 (rank_score <= 9)
    - 每年1、4、12月：空仓不交易（有持仓则当天清仓，不再新开仓）
    """
    # 如果没有预先计算的数据，则直接返回
    if not hasattr(context, "data") or context.data is None:
        return

    today = data.current_dt
    today_str = today.strftime("%Y-%m-%d")
    month_num = today.month

    # 1/4/12 月份：空仓不交易
    if month_num in (1, 4, 12):
        positions = context.get_positions()
        if positions:
            for inst in list(positions.keys()):
                context.order_target_percent(inst, 0)
        return

    # 其他月份正常交易
    df_all = context.data
    df_today_all = df_all[df_all["date"] == today_str]

    # 今天没有任何评分数据，则不交易
    if df_today_all.empty:
        return

    top_hold = context.top_hold          # 5
    keep_threshold = context.keep_threshold  # 9

    # 今天排名前 keep_threshold 名（<=9）的股票
    df_today = df_today_all[df_today_all["rank_score"] <= keep_threshold].copy()
    if df_today.empty:
        return

    # 买入候选：排名前 top_hold 名（<=5）
    df_buy = df_today[df_today["rank_score"] <= top_hold].copy()

    # 1) 卖出逻辑
    positions = context.get_positions()
    for inst, pos in positions.items():
        rec = df_today[df_today["instrument"] == inst]
        if rec.empty:
            # 不在前9名 => 掉出第9名，直接卖出
            context.order_target_percent(inst, 0)
            continue

        rank = rec["rank_score"].iloc[0]
        limit_status = rec["price_limit_status"].iloc[0]

        # 涨停则不卖
        if limit_status == 3:
            continue

        # 未涨停，且排在9名之外 => 卖出
        if rank > keep_threshold:
            context.order_target_percent(inst, 0)
        # 否则 rank<=9 且未涨停 => 不卖

    # 2) 买入/调仓逻辑：只对前 top_hold 名且未跌停的股票下单
    target_weight_each = 1.0 / top_hold

    for _, row in df_buy.iterrows():
        inst = row["instrument"]
        limit_status = row["price_limit_status"]

        # 买入条件：调仓日所选股票没有跌停 => price_limit_status != 1
        if limit_status == 1:
            continue

        context.order_target_percent(inst, target_weight_each)

# 运行回测
performance = bigtrader.run(
    market=bigtrader.Market.CN_STOCK,
    frequency=bigtrader.Frequency.DAILY,
    start_date="2024-01-01",
    end_date="2025-03-07",
    capital_base=1000000,
    initialize=initialize,
    handle_data=handle_data,
    order_price_field_buy="close",   # 用收盘价近似 14:55 成交
    order_price_field_sell="close",
)

performance.render()