from bigmodule import M

# <aistudiograph>

# @param(id="m7", name="run")
def m7_run_bigquant_run(input_1, input_2, input_3):
    
    import dai
    
    sql1 = """ 
    
    WITH 
    t1 AS (
        SELECT 
            total_shares, 
            publish_date as date, 
            instrument 
        FROM cn_stock_capital 
        WHERE change_date = date
    ), 

    t2 AS (
        SELECT 
            date,
            instrument,
            cash_before_tax 
        FROM cn_stock_dividend
    ),

    t3 AS (
        SELECT 
            date, 
            instrument, 
            t2.cash_before_tax AS cash_before_tax, 
            t1.total_shares AS total_shares  
        FROM t2 FULL JOIN t1 USING (date, instrument)
        ORDER BY date, instrument
    ),

    t4 AS (
        SELECT 
            date, 
            instrument,  
            LAST(COLUMNS([cash_before_tax, total_shares]) IGNORE NULLS)  OVER (PARTITION BY instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 'COLUMNS(*)',
        FROM t3
    )

    SELECT 
        date, 
        instrument, 
        cash_before_tax * total_shares AS dividend_amount  
    FROM t4 
    ORDER BY date ,instrument

    """
    
    devidend_df = dai.query(sql1, filters={"date": ["2010-01-01", "2024-05-31"],  "publish_date": ["2010-01-01", "2024-02-01"]}).df()
    
    sql2 = """

    SELECT 
        date,
        instrument, 
        AVG(net_profit_ly) AS avg_net_profit 
    FROM cn_stock_financial_ly_shift 
    WHERE shift <= 2
    GROUP BY date, instrument
    ORDER BY date, instrument

    """ 

    net_profit_df=dai.query(sql2, filters={"date": ["2010-01-01", "2024-05-31"]}).df()
    
    sql3 = """ 
    SELECT 
        date, 
        instrument, 
        avg_net_profit, 
        devidend_df.dividend_amount 
    FROM net_profit_df 
    FULL JOIN devidend_df USING (date, instrument)
    FULL JOIN cn_stock_prefactors USING (date, instrument)
    ORDER BY date, instrument
    """
    merge_df = dai.query(sql3, filters={"date": ["2010-01-01", "2024-05-31"]}).df()
    
    sql4 = """  
        WITH 
        t0 AS (
        SELECT 
            date, 
            instrument,  
            LAST(COLUMNS([avg_net_profit, dividend_amount]) IGNORE NULLS) OVER (PARTITION BY instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 'COLUMNS(*)',
        FROM merge_df
        ) 
        SELECT 
            date, 
            instrument, 
            avg_net_profit, 
            m_sum(dividend_amount, 750) AS y3_dividend_amount 
        FROM t0 
        ORDER BY date, instrument

    """
    fillna_df = dai.query(sql4, filters={"date": ["2010-01-01", "2024-05-31"]}).df()
 


    ds = dai.DataSource.write_bdb(fillna_df)

    return dict(data_1=ds, data_2={"hello": "world"}, data_3=None)

# @param(id="m7", name="post_run_outputs_")
def m7_post_run_outputs__bigquant_run(outputs):
    # 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
    return outputs

# @param(id="m5", name="initialize")
# 交易引擎：初始化函数，只执行一次
def m5_initialize_bigquant_run(context):
    # 加载预测数据
    pass

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
# 回测引擎：每日数据处理函数, 每天执行一次
def m5_handle_data_bigquant_run(context, data):
    import pandas as pd

    # 下一个交易日不是调仓日，则不生成信号
    if not context.rebalance_period.is_signal_date(data.current_dt.date()):
        return

    # 从传入的数据 context.data 中读取今天的信号数据
    today_df = context.data[context.data["date"] == data.current_dt.strftime("%Y-%m-%d")]

    # 卖出不在目标持有列表中的股票
    for instrument in sorted(set(context.get_account_positions().keys()) - set(today_df["instrument"])):
        context.order_target_percent(instrument, 0)
    # 买入目标持有列表中的股票
    for i, x in today_df.iterrows():
        context.order_target_percent(x.instrument, 0.0 if pd.isnull(x.position) else x.position)
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

# @module(position="-541,-1079", comment="""可以根据基础信息目标进行筛选：如上市板块、交易所信息等""")
m1 = M.cn_stock_basic_selector.v7(
    exchanges=["""上交所""", """深交所"""],
    list_sectors=["""主板""", """创业板""", """科创板"""],
    indexes=["""中证500""", """上证指数""", """创业板指""", """深证成指""", """北证50""", """上证50""", """科创50""", """沪深300""", """中证1000""", """中证100""", """深证100"""],
    st_statuses=["""正常"""],
    margin_tradings=["""两融标的""", """非两融标的"""],
    sw2021_industries=["""农林牧渔""", """采掘""", """基础化工""", """钢铁""", """有色金属""", """建筑建材""", """机械设备""", """电子""", """汽车""", """交运设备""", """信息设备""", """家用电器""", """食品饮料""", """纺织服饰""", """轻工制造""", """医药生物""", """公用事业""", """交通运输""", """房地产""", """金融服务""", """商贸零售""", """社会服务""", """信息服务""", """银行""", """非银金融""", """综合""", """建筑材料""", """建筑装饰""", """电力设备""", """国防军工""", """计算机""", """传媒""", """通信""", """煤炭""", """石油石化""", """环保""", """美容护理"""],
    drop_suspended=True,
    m_name="""m1"""
)

# @module(position="-539,-912", comment="""输入因子表达式特征，记为score，直接用于打分""")
m2 = M.input_features_dai.v30(
    input_1=m1.data,
    mode="""表达式""",
    expr="""total_market_cap AS score
net_profit_deducted_lf""",
    expr_filters="""list_days > 270


-- 过滤市值连续20个交易日低于5亿元的公司
m_min(total_market_cap, 20) > 500000000

-- 过滤财务状况不达标的公司（利润总额、净利润或扣非净利润为负值，且营业收入低于3亿元）
NOT ((total_profit_ttm < 0 OR net_profit_deducted_ttm < 0 OR net_profit_to_parent_deducted_ttm < 0) AND operating_revenue_yoy_ttm < 300000000)

-- 过滤如果公司最近一个会计年度净利润为正值，但三年分红低于年均净利润的30%，且累计分红金额低于5,000万元，可能会被实施ST。
-- (net_profit_deducted_lf <= 0 OR (dividend_yield_ratio >= 0.3 * net_profit_deducted_ttm) AND m_sum(dividend_yield_ratio, 3) >= 50000000))""",
    expr_tables="""cn_stock_prefactors cn_stock_factors_financial_indicators""",
    extra_fields="""date, instrument""",
    order_by="""date, instrument""",
    expr_drop_na=True,
    sql="""-- 使用DAI SQL获取数据, 构建因子等, 如下是一个例子作为参考
-- DAI SQL 语法: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-sql%E5%85%A5%E9%97%A8%E6%95%99%E7%A8%8B
-- 使用数据输入1/2/3里的字段: e.g. input_1.close, input_1.* EXCLUDE(date, instrument)

SELECT
    -- 在这里输入因子表达式
    -- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
    -- 数据&字段: 数据文档 https://bigquant.com/data/home

    m_lag(close, 90) / close AS return_90,
    m_lag(close, 30) / close AS return_30,
    -- 下划线开始命名的列是中间变量, 不会在最终结果输出 (e.g. _rank_return_90)
    c_pct_rank(-return_90) AS _rank_return_90,
    c_pct_rank(return_30) AS _rank_return_30,

    c_rank(volume) AS rank_volume,
    close / m_lag(close, 1) as return_0,

    -- 日期和股票代码
    date, instrument
FROM
    -- 预计算因子 cn_stock_bar1d https://bigquant.com/data/datasources/cn_stock_bar1d
    cn_stock_factors
    -- SQL 模式不会自动join输入数据源, 可以根据需要自由灵活的使用
    -- JOIN input_1 USING(date, instrument)
WHERE
    -- WHERE 过滤, 在窗口等计算算子之前执行
    -- 剔除ST股票
    st_status = 0
QUALIFY
    -- QUALIFY 过滤, 在窗口等计算算子之后执行, 比如 m_lag(close, 3) AS close_3, 对于 close_3 的过滤需要放到这里
    -- 去掉有空值的行
    COLUMNS(*) IS NOT NULL
    -- _rank_return_90 是窗口函数结果，需要放在 QUALIFY 里
    AND _rank_return_90 > 0.1
    AND _rank_return_30 < 0.1
-- 按日期和股票代码排序, 从小到大
ORDER BY date, instrument
""",
    extract_data=False,
    m_name="""m2"""
)

# @module(position="-538,-779", comment="""抽取预测数据""")
m3 = M.extract_data_dai.v17(
    sql=m2.data,
    start_date="""2021-01-01""",
    start_date_bound_to_trading_date=True,
    end_date="""2024-05-31""",
    end_date_bound_to_trading_date=True,
    before_start_days=90,
    debug=False,
    m_name="""m3"""
)

# @module(position="-68,-911", comment="""数据进行填充""")
m7 = M.python.v2(
    run=m7_run_bigquant_run,
    do_run=True,
    post_run_outputs_=m7_post_run_outputs__bigquant_run,
    m_name="""m7"""
)

# @module(position="-384,-670", comment="""""", comment_collapsed=True)
m10 = M.data_join.v4(
    data1=m3.data,
    data2=m7.data_1,
    on="""date,instrument""",
    how="""inner""",
    sort=False,
    m_name="""m10"""
)

# @module(position="-382,-580", comment="""""", comment_collapsed=True)
m11 = M.data_filter.v5(
    input_data=m10.data,
    expr="""(net_profit_deducted_lf >0 ) & (y3_dividend_amount > avg_net_profit*0.3) & ( y3_dividend_amount>5000)""",
    output_left_data=False,
    m_name="""m11"""
)

# @module(position="-380,-469", comment="""持股数量、打分到仓位""")
m4 = M.score_to_position.v3(
    input_1=m11.data,
    score_field="""score ASC""",
    hold_count=30,
    position_expr="""1 AS position
""",
    total_position=1,
    extract_data=True,
    m_name="""m4"""
)

# @module(position="-382,-334", comment="""交易，日线，设置初始化函数和K线处理函数，以及初始资金、基准等""")
m5 = M.bigtrader.v25(
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
    rebalance_period_days="""5""",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="""标准模式""",
    before_start_days=0,
    volume_limit=1,
    order_price_field_buy="""open""",
    order_price_field_sell="""close""",
    benchmark="""沪深300指数""",
    plot_charts=True,
    debug=False,
    backtest_only=False,
    m_name="""m5"""
)
# </aistudiograph>