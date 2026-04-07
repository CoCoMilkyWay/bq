from bigmodule import M, I

# <aistudiograph>

# @param(id="m5", name="initialize")
# 交易引擎：初始化函数，只执行一次
def m5_initialize_bigquant_run(context):
    from bigtrader.finance.commission import PerOrder

    # 系统已经设置了默认的交易手续费和滑点，要修改手续费可使用如下函数
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

    # ===== 空仓月 =====
    if data.current_dt.month in [1,4,12]:
        for s in context.get_account_positions():
            context.order_target_percent(s, 0)
        return

    # ===== 调仓判断 =====
    if not context.rebalance_period.is_signal_date(data.current_dt.date()):
        return

    df = context.data[context.data["date"] == today]
    if df.empty:
        return

    BUY_N = 5
    KEEP_N = 9

    df = df.sort_values("rank_score")

    buy_df = df.head(BUY_N)
    buffer_set = set(df.head(KEEP_N)["instrument"])

    holding = set(context.get_account_positions().keys())

    # ===== 卖出 =====
    for stock in holding:
        bar = data.current(stock, ["close", "high_limit"])

        # 涨停不卖
        if abs(bar["close"] - bar["high_limit"]) < 1e-6:
            continue

        if stock not in buffer_set:
            context.order_target_percent(stock, 0)

    # ===== 买入 =====
    buy_list = []

    for _, row in buy_df.iterrows():
        stock = row["instrument"]
        bar = data.current(stock, ["close", "low_limit"])

        # 跌停不买
        if bar["close"] <= bar["low_limit"]:
            continue

        buy_list.append(stock)

    if not buy_list:
        return

    weight = 1.0 / len(buy_list)

    for stock in buy_list:
        context.order_target_percent(stock, weight)

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

# @module(position="-691,-915", comment="""使用基本信息对股票池过滤""")
m1 = M.cn_stock_basic_selector.v8(
    exchanges=["""上交所""", """深交所"""],
    list_sectors=["""主板"""],
    st_statuses=["""正常"""],
    margin_tradings=["""两融标的""", """非两融标的"""],
    sw2021_industries=["""农林牧渔""", """采掘""", """基础化工""", """钢铁""", """有色金属""", """建筑建材""", """机械设备""", """电子""", """汽车""", """交运设备""", """信息设备""", """家用电器""", """食品饮料""", """纺织服饰""", """轻工制造""", """医药生物""", """公用事业""", """交通运输""", """房地产""", """金融服务""", """商贸零售""", """社会服务""", """信息服务""", """银行""", """非银金融""", """综合""", """建筑材料""", """建筑装饰""", """电力设备""", """国防军工""", """计算机""", """传媒""", """通信""", """煤炭""", """石油石化""", """环保""", """美容护理"""],
    drop_suspended=True,
    m_name="""m1"""
)

# @module(position="-681,-812", comment="""因子特征""")
m2 = M.input_features_dai.v30(
    input_1=m1.data,
    mode="""SQL""",
    expr="""-- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
-- 数据&字段: 数据文档 https://bigquant.com/data/home
-- 数据使用: 表名.字段名, 对于没有指定表名的列, 会从 expr_tables 推断, 如果同名字段在多个表中出现, 需要显式的给出表名

m_lag(close, 90) / close AS return_90
m_lag(close, 30) / close AS return_30
-- cn_stock_bar1d.close / cn_stock_bar1d.open
-- cn_stock_prefactors https://bigquant.com/data/datasources/cn_stock_prefactors 是常用因子表(VIEW), JOIN了很多数据表, 性能会比直接用相关表慢一点, 但使用简单
-- cn_stock_prefactors.pe_ttm

-- 表达式模式下, 会自动join输入数据1/2/3, 可以在表达式里直接使用其字段。包括 input_1 的所有列但去掉 date, instrument。注意字段不能有重复的, 否则会报错
-- input_1.* EXCLUDE(date, instrument)
-- input_1.close
-- input_2.close / input_1.close
""",
    expr_filters="""-- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
-- 数据&字段: 数据文档 https://bigquant.com/data/home
-- 表达式模式的过滤都是放在 QUALIFY 里, 即数据查询、计算, 最后才到过滤条件

-- c_pct_rank(-return_90) <= 0.3
-- c_pct_rank(return_30) <= 0.3
-- cn_stock_bar1d.turn > 0.02
""",
    expr_tables="""cn_stock_prefactors""",
    extra_fields="""date, instrument""",
    order_by="""date, instrument""",
    expr_drop_na=True,
    sql="""
WITH base AS (
    SELECT
        date,
        instrument,
        close,
        total_market_cap,
        float_market_cap,
        net_cffoa_ttm,
        net_profit_ttm,
        st_status,
        list_sector,
        price_limit_status,

        m_lag(net_profit_ttm, 252) AS net_profit_ttm_1y_ago,
        m_lag(net_cffoa_ttm, 252) AS net_cffoa_ttm_1y_ago,

        (close / m_lag(close, 5) - 1) AS ret_5d
    FROM cn_stock_prefactors
),

filtered AS (
    SELECT *,
        CASE
            WHEN net_cffoa_ttm_1y_ago IS NOT NULL
                 AND net_cffoa_ttm_1y_ago != 0
            THEN net_cffoa_ttm / net_cffoa_ttm_1y_ago - 1
            ELSE NULL
        END AS growth_cffoa_12m
    FROM base
    WHERE
        list_sector NOT IN (2,3,4)
        AND st_status = 0
        AND net_profit_ttm > 0
        AND net_profit_ttm_1y_ago > 0
        AND (ret_5d IS NULL OR ret_5d <= 0.5)
),

ranked AS (
    SELECT
        date,
        instrument,
        close,
        price_limit_status,

        c_rank(growth_cffoa_12m) AS rk_cffoa,
        c_rank(total_market_cap) AS rk_total,
        c_rank(float_market_cap) AS rk_float,
        c_rank(close) AS rk_price
    FROM filtered
),

scored AS (
    SELECT *,
        (1*rk_cffoa + 7*rk_total + 7*rk_float + 1*rk_price) AS score
    FROM ranked
),

ranked2 AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY score ASC) AS rank_score
    FROM scored
)

SELECT *
FROM ranked2
WHERE 
    EXTRACT(MONTH FROM date) NOT IN (1,4,12)
    AND date >= '2020-01-01'   -- ⭐ 必加！！！
ORDER BY date, rank_score
""",
    extract_data=False,
    m_name="""m2"""
)

# @module(position="-663,-716", comment="""持股数量、打分到仓位""")
m3 = M.score_to_position.v5(
    input_1=m2.data,
    score_field="""rank_score ASC""",
    hold_count=5,
    position_expr="""1 AS position""",
    total_position=1,
    extract_data=False,
    m_name="""m3"""
)

# @module(position="-638,-619", comment="""抽取预测数据""")
m4 = M.extract_data_dai.v20(
    sql=m3.data,
    start_date="""2017-01-01""",
    start_date_bound_to_trading_date=True,
    end_date="""2025-11-30""",
    end_date_bound_to_trading_date=True,
    before_start_days=90,
    keep_before=False,
    debug=False,
    m_name="""m4"""
)

# @module(position="-615,-513", comment="""交易，日线，设置初始化函数和K线处理函数，以及初始资金、基准等""")
m5 = M.bigtrader.v53(
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
    capital_base=200000,
    frequency="""daily""",
    product_type="""股票""",
    rebalance_period_type="""交易日""",
    rebalance_period_days="""1""",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="""标准模式""",
    before_start_days=0,
    volume_limit=1,
    order_price_field_buy="""open""",
    order_price_field_sell="""open""",
    benchmark="""沪深300指数""",
    plot_charts="""全部显示""",
    debug=False,
    backtest_only=False,
    m_name="""m5"""
)
# </aistudiograph>