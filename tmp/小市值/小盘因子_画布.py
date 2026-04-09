from bigmodule import M

# <aistudiograph>

# @param(id="m5", name="initialize")
# 交易引擎：初始化函数，只执行一次
def m5_initialize_bigquant_run(context):
    from bigtrader.finance.commission import PerOrder

    # 这里沿用模板设置的交易手续费
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))

# @param(id="m5", name="before_trading_start")
# 交易引擎：每个单位时间开盘前调用一次。
def m5_before_trading_start_bigquant_run(context, data):
    # 盘前处理，订阅行情等
    pass

# @param(id="m5", name="handle_tick")
# 交易引擎：tick数据处理函数，每个tick执行一次
def m5_handle_tick_bigquant_run(context, tick):
    pass

# @param(id="m5", name="handle_data")
def m5_handle_data_bigquant_run(context, data):
    import pandas as pd

    today = data.current_dt.strftime("%Y-%m-%d")

    # ===== 1. 月份空仓控制 =====
    month = data.current_dt.month

    if month in [1, 4, 12]:
        positions = context.get_account_positions()

        for stock in positions:
            bar = data.current(stock, ["close", "high_limit"])

            # 涨停不卖
            if abs(bar["close"] - bar["high_limit"]) < 1e-6:
                continue

            context.order_target_percent(stock, 0)

        return

    # ===== 2. 调仓日判断 =====
    if not context.rebalance_period.is_signal_date(data.current_dt.date()):
        return

    # ===== 3. 获取当日数据 =====
    df = context.data[context.data["date"] == today]

    if df.empty:
        return

    # ===== 4. 排序（score 越小越好）=====
    df = df.sort_values(by="score", ascending=True)

    BUY_N = 5
    BUFFER_N = 10

    # 缓冲池
    buffer_set = set(df.head(BUFFER_N)["instrument"])

    # 当前持仓
    positions = context.get_account_positions()
    holding = set(positions.keys())

    # ===== 5. 选出可买股票（避免跌停）=====
    final_buy = []

    for stock in df["instrument"]:
        if len(final_buy) >= BUY_N:
            break

        bar = data.current(stock, ["close", "low_limit"])

        # 跌停不买
        if bar["close"] <= bar["low_limit"]:
            continue

        final_buy.append(stock)

    # 权重（动态，防止买不满）
    weight = 1.0 / len(final_buy) if final_buy else 0
    target_weights = {s: weight for s in final_buy}

    # ===== 6. 卖出逻辑 =====
    for stock in holding:
        bar = data.current(stock, ["close", "high_limit"])

        # 涨停不卖
        if abs(bar["close"] - bar["high_limit"]) < 1e-6:
            continue

        # 不在缓冲区才卖
        if stock not in buffer_set:
            context.order_target_percent(stock, 0)

    # ===== 7. 买入 + 调仓 =====
    for stock, w in target_weights.items():
        context.order_target_percent(stock, w)


# @param(id="m5", name="handle_trade")
# 交易引擎：成交回报处理函数，每个成交发生时执行一次
def m5_handle_trade_bigquant_run(context, trade):
    pass

# @param(id="m5", name="handle_order")
# 交易引擎：委托回报处理函数，每个委托变化时执行一次
def m5_handle_order_bigquant_run(context, order):
    pass

# @param(id="m5", name="after_trading")
# 交易引擎：盘后处理函数，每日盘后执行一次
def m5_after_trading_bigquant_run(context, data):
    pass

# @module(position="140,40", comment="""使用基本信息对股票池过滤，仅保留主要指数成分股、小市值候选池""")
m1 = M.cn_stock_basic_selector.v8(
    exchanges=["""上交所""", """深交所"""],
    list_sectors=["""主板""", """创业板"""],
    st_statuses=["""正常"""],
    drop_suspended=True,
    m_name="""m1"""
)

# @module(position="140,170", comment="""因子特征：使用流通市值作为打分因子，并进行基础过滤""")
m2 = M.input_features_dai.v30(
    input_1=m1.data,
    mode="""表达式""",
    expr="""float_market_cap AS score""",
    expr_filters="""list_days > 365 AND pe_ttm > 0 AND st_status = 0 AND suspended = 0""",
    expr_tables="""cn_stock_prefactors""",
    extra_fields="""date, instrument""",
    order_by="""score ASC""",
    expr_drop_na=True,
    sql="""-- 使用DAI SQL获取数据, 构建因子等, 如下是一个例子作为参考

SELECT
    -- ===== 排名条件（按图片完全一致）=====

    -- 总市值（从小到大）
    c_pct_rank(total_market_cap) AS _rank_total_mkt_cap,

    -- 中性SP（从大到小）
    c_pct_rank(-sp) AS _rank_sp,

    -- 中性CP（从大到小）
    c_pct_rank(-cp) AS _rank_cp,

    -- 流通市值（从小到大）
    c_pct_rank(float_market_cap) AS _rank_float_mkt_cap,

    -- 近期配股标记（从小到大）
    c_pct_rank(placement_flag) AS _rank_placement,

    -- 未来20日新增流通股占比（从小到大）
    c_pct_rank(future_20d_unlock_ratio) AS _rank_unlock,

    -- ===== 综合打分（等权）=====
    (
        _rank_total_mkt_cap +
        _rank_sp +
        _rank_cp +
        _rank_float_mkt_cap +
        _rank_placement +
        _rank_unlock
    ) / 6 AS score,

    date,
    instrument
FROM
    cn_stock_prefactors

WHERE
    -- WHERE 过滤, 在窗口等计算算子之前执行
    -- 剔除ST股票
    st_status = 0
    --AND list_days > 365
    --AND pe_ttm > 0
    AND suspended = 0
    AND net_profit_ttm > 0
    AND m_lag(net_profit_ttm, 252) > 0
    AND (close / m_lag(close, 5) - 1) <= 0.5


QUALIFY
    COLUMNS(*) IS NOT NULL

-- 按日期和股票代码排序, 从小到大
ORDER BY date, score ASC
""",
    extract_data=False,
    m_name="""m2"""
)

# @module(position="140,300", comment="""持股数量控制与打分到仓位映射""")
m3 = M.score_to_position.v4(
    input_1=m2.data,
    score_field="""score""",
    hold_count=5,
    position_expr="""1 AS position""",
    total_position=1,
    extract_data=False,
    m_name="""m3"""
)

# @module(position="140,430", comment="""抽取预测数据，限定回测时间区间""")
m4 = M.extract_data_dai.v20(
    sql=m3.data,
    start_date="""2017-01-01""",
    start_date_bound_to_trading_date=True,
    end_date="""2025-12-31""",
    end_date_bound_to_trading_date=True,
    before_start_days=90,
    keep_before=False,
    debug=False,
    m_name="""m4"""
)

# @module(position="140,560", comment="""交易引擎：日线回测，1日调仓，小市值成分股策略""")
m5 = M.bigtrader.v43(
    data=m4.data,
    start_date="""""",
    end_date="""""",
    initialize=m5_initialize_bigquant_run,
    before_trading_start=m5_before_trading_start_bigquant_run,
    handle_tick=m5_handle_tick_bigquant_run,
    handle_data=m5_handle_data_bigquant_run,
    handle_trade=m5_handle_trade_bigquant_run,
    handle_order=m5_handle_order_bigquant_run,
    after_trading=m5_after_trading_bigquant_run,
    capital_base=1000000,
    frequency="""daily""",
    product_type="""股票""",
    rebalance_period_type="""交易日""",
    rebalance_period_days="""1""",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="""标准模式""",
    before_start_days=0,
    volume_limit=1,
    order_price_field_buy="""close""",
    order_price_field_sell="""close""",
    benchmark="""沪深300指数""",
    plot_charts=True,
    debug=False,
    backtest_only=False,
    m_name="""m5"""
)
# </aistudiograph>