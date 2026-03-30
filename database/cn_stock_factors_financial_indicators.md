首页
编写策略
数据平台
策略社区
我的交易
宽客学院
知识库
Pro会员
bqkemtr7
数据平台
数据平台/
量化因子/
股票因子/
cn_stock_factors_financial_indicators
财务指标(cn_stock_factors_financial_indicators)
数据描述
该表提供了A股市场上市公司的核心财务指标（日频），包括毛利润、经营活动净收益、价值变动净收益、利息费用、扣非净利润、基本每股收益，以及部分指标的复合增长率等关键指标的最新一期、单季度、滚动十二期数据。这些财务指标能够用于评估公司的成长能力、资本结构、偿债能力、盈利能力和收益质量等。

表结构
文档
用例
更新记录
分组
请选择分组过滤
显示 
50
 项结果
输入搜索关键词
字段	字段类型	字段描述
date	TIMESTAMP_NS	日期
instrument	VARCHAR	证券代码
gross_profit_lf	DOUBLE	毛利润(最新一期)
gross_profit_mrq	DOUBLE	毛利润(单季度)
gross_profit_ttm	DOUBLE	毛利润(滚动十二期)
operating_net_income_lf	DOUBLE	经营活动净收益(最新一期)
operating_net_income_mrq	DOUBLE	经营活动净收益(单季度)
operating_net_income_ttm	DOUBLE	经营活动净收益(滚动十二期)
value_chg_net_income_lf	DOUBLE	价值变动净收益(最新一期)
value_chg_net_income_mrq	DOUBLE	价值变动净收益(单季度)
value_chg_net_income_ttm	DOUBLE	价值变动净收益(滚动十二期)
interest_expense_lf	DOUBLE	利息费用(最新一期)
interest_expense_mrq	DOUBLE	利息费用(单季度)
interest_expense_ttm	DOUBLE	利息费用(滚动十二期)
depreciation_amortization_lf	DOUBLE	当期计提折旧与摊销(最新一期)
depreciation_amortization_mrq	DOUBLE	当期计提折旧与摊销(单季度)
depreciation_amortization_ttm	DOUBLE	当期计提折旧与摊销(滚动十二期)
effect_tax_rate_lf	DOUBLE	有效税率(最新一期)
effect_tax_rate_mrq	DOUBLE	有效税率(单季度)
effect_tax_rate_ttm	DOUBLE	有效税率(滚动十二期)
noninterest_curr_liabilities_lf	DOUBLE	无息流动负债(最新一期)
noninterest_noncurr_liabilities_lf	DOUBLE	无息非流动负债(最新一期)
ebit_lf	DOUBLE	息税前利润(最新一期)
ebit_mrq	DOUBLE	息税前利润(单季度)
ebit_ttm	DOUBLE	息税前利润(滚动十二期)
ebitda_lf	DOUBLE	息税折旧摊销前利润(最新一期)
ebitda_mrq	DOUBLE	息税折旧摊销前利润(单季度)
ebitda_ttm	DOUBLE	息税折旧摊销前利润(滚动十二期)
nopat_lf	DOUBLE	税后净营业利润(最新一期)
nopat_mrq	DOUBLE	税后净营业利润(单季度)
nopat_ttm	DOUBLE	税后净营业利润(滚动十二期)
interest_bearing_debt_lf	DOUBLE	带息债务(最新一期)
shortterm_debt_lf	DOUBLE	短期债务(最新一期)
longterm_liabilities_lf	DOUBLE	长期负债(最新一期)
invested_capital_lf	DOUBLE	全部投入资本(最新一期)
working_capital_lf	DOUBLE	营运资本(最新一期)
net_working_capital_lf	DOUBLE	净营运资本(最新一期)
tangible_assets_lf	DOUBLE	有形资产(最新一期)
retained_income_lf	DOUBLE	留存收益(最新一期)
net_debt_lf	DOUBLE	净债务(最新一期)
inc_wc_lf	DOUBLE	营运资本增加(最新一期)
fcff_lf	DOUBLE	企业自由现金流(最新一期)
fcff_mrq	DOUBLE	企业自由现金流(单季度)
fcff_ttm	DOUBLE	企业自由现金流(滚动十二期)
fcfe_lf	DOUBLE	股权自由现金流(最新一期)
fcfe_mrq	DOUBLE	股权自由现金流(单季度)
fcfe_ttm	DOUBLE	股权自由现金流(滚动十二期)
inc_inventory_lf	DOUBLE	存货的增加(最新一期)
longterm_invest_lf	DOUBLE	长期投资总额(最新一期)
net_profit_deducted_lf	DOUBLE	扣非净利润(最新一期)
显示第 1 至 50 项结果，共 2,037 项
上页12345…41下页
表名
cn_stock_factors_financial_indicators
作者
BigQuant
L
v
36
最近更新时间
2026-03-27 17:33:59
续订数据
%