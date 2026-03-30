import pandas as pd
import numpy as np
import warnings
import empyrical
import dai
import bigcharts
import time 
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')

### 因子分析工具
class AlphaMiner(object):

    def __init__(self, params, version="sql"):

        t0 = time.time()
        self.params = params
        self.params['alpha_id'] = 'alpha_' + self.params['alpha_class'] + '_' + self.params['alpha_name']
        self.sd = '2018-04-25'
        self.ed = datetime.now().date().strftime("%Y-%m-%d")
        print('====================因子分析开始====================')
        
        # 1. 获取因子数据
        self.factor_data = self.get_factor_data(self.params['data_process'], self.params['instruments'], self.params['alpha_sql'])
        print("1. 因子数据获取完成, 总耗时", round(time.time()-t0, 2), "秒")

        # 2. 按因子分组及计算收益率
        self.group_data = self.get_group_data()
        print("2. 个股收益计算完成, 总耗时", round(time.time()-t0, 2), "秒")

        # 3. 分组收益率与分组累计收益率计算
        self.group_ret, self.group_cumret = self.get_group_cumret()
        print("3. 分组收益计算完成, 总耗时", round(time.time()-t0, 2), "秒")

        # 4. 综合收益与IC指标计算
        self.whole_perf = self.get_whole_perf() 
        self.yearly_perf =  self.get_yearly_perf() 
        self.ic = self.get_all_ic() 
        print("4. 综合收益与IC指标计算完成, 总耗时", round(time.time()-t0, 2), "秒")

        print("====================因子分析完成====================")
        print("请使用 render(local_plot=True)  方法展示因子绩效图表")
        print("请使用 submit(local_plot=False) 方法将因子上传至因子平台")


    # 一、因子数据获取
    def get_factor_data(self, data_process, pool_class, alpha_sql):
        pool_dict = {
            "中证500":"is_zz500 = 1",
            "中证1000":"is_zz1000 = 1",
            "沪深300":"is_hs300 = 1",
            "全市场":"1=1",
        }
        data_process_sql = "SELECT date, instrument, neutralized_factor AS factor FROM data_alpha_process ORDER BY date, instrument" if data_process == True else "SELECT * FROM data_alpha"
        sql = f"""
            WITH
            data_alpha AS (
                {alpha_sql}
            ),
            data_alpha_origin AS (
                SELECT *
                FROM data_alpha
                QUALIFY COLUMNS(*) IS NOT NULL AND factor != 'Infinity' AND factor != '-Infinity'
            ),
            data_alpha_process AS (
                SELECT 
                    date,
                    instrument,
                    factor,
                    clip(factor, c_avg(factor) - 3 * c_std(factor), c_avg(factor) + 3 * c_std(factor)) AS clipped_factor,
                    c_normalize(clipped_factor) AS normalized_factor,
                    c_neutralize(normalized_factor, sw2021_level1, LOG(total_market_cap)) AS neutralized_factor,
                FROM data_alpha_origin JOIN cn_stock_factors_base USING (date, instrument)
                WHERE 1=1
                AND {pool_dict[pool_class]}
                AND amount > 0
                AND st_status = 0
                AND trading_days > 252
                AND (instrument LIKE '%SH' OR instrument LIKE '%SZ')
                QUALIFY COLUMNS(*) IS NOT NULL
                ORDER BY date, instrument
            )
            {data_process_sql}
        """
        df = dai.query(sql, filters={'date':[self.sd, self.ed]}).df()
        return df

    # 二、按因子分组及计算收益率
    def get_group_data(self):
        # 获取每日收益率
        def get_daily_ret():
            sql = f"""
                SELECT 
                    date, 
                    instrument, 
                    (m_lead(open, 2)/ m_lead(open, 1) - 1) AS daily_ret 
                FROM cn_stock_bar1d 
                WHERE date BETWEEN DATE '{self.sd}' - INTERVAL 10 DAY AND '{self.ed}'
                ORDER BY date, instrument
            """
            daily_ret_data = dai.query(sql).df()
            return daily_ret_data
        # 因子分组
        def cut(df, group_num=10):
            df = df.drop_duplicates('factor') 
            df['group'] = pd.qcut(df['factor'], q=group_num, labels=False, duplicates='drop')
            df = df.dropna(subset=['group'], how='any')
            df['group'] = df['group'].apply(int).apply(str)
            return df
        # 收益率计算及分组
        daily_ret_data = get_daily_ret()
        merge_data = pd.merge(self.factor_data.sort_values(['date', 'instrument']), daily_ret_data.sort_values(['date', 'instrument']), on=['date','instrument'], how='left')
        group_data = merge_data.groupby('date', group_keys=False).apply(cut, group_num=self.params['group_num'])
        return group_data

    # 三、分组收益率与分组累计收益率计算
    def get_group_cumret(self):
        # 获取基准收益率
        def get_bm_ret():
            bm_dict = {
            "中证500":"000905.SH",
            "中证1000":"000852.SH",
            "沪深300":"000300.SH",
            }
            sql = f"""
            SELECT 
                date,instrument, (close - m_Lag(close,1))  / m_LAG(close, 1) as benchmark_ret
            FROM cn_stock_index_bar1d
            WHERE date BETWEEN DATE '{self.sd}' - INTERVAL 10 DAY AND '{self.ed}'
            AND instrument = '{bm_dict[self.params['benchmark']]}'
            """
            bm_ret = dai.query(sql).df()
            return bm_ret
        # 基准收益率
        bm_ret = get_bm_ret()
        bm_ret = bm_ret.set_index('date')
        # 分组收益率
        groupret_data = self.group_data[['date','group','daily_ret']].groupby(['date','group'], group_keys=False).apply(lambda x:np.nanmean(x)).reset_index()
        groupret_data.rename(columns={0:'g_ret'}, inplace=True)
        groupret_pivotdata = groupret_data.pivot(index='date', values='g_ret', columns='group')
        groupret_pivotdata['ls'] = groupret_pivotdata[str(self.params['group_num']-1)] - groupret_pivotdata['0']
        groupret_pivotdata['bm'] = bm_ret['benchmark_ret'] 
        groupret_pivotdata = groupret_pivotdata.shift(1)
        # 分组累计收益率
        groupcumret_pivotdata = groupret_pivotdata.cumsum() 
        # 返回分组收益率与分组累计收益率
        return groupret_pivotdata, groupcumret_pivotdata

    # 四、综合收益与IC指标计算
    # （一）综合收益
    def get_whole_perf(self):
        # 各投资组合代号
        ll_pos = str(self.params['group_num']-1)
        ss_pos = "0"
        ls_pos = "ls"
        bm_pos = "bm"
        # 1. 收益绩效
        def get_basic_perf(data_type):
            def cal_stats(series, bm_series):
                series = series.fillna(0)
                trading_days = len(series)
                return_ratio =  series.sum()
                annual_return_ratio = series.sum() * 242 / trading_days 
                ex_return_ratio =  (series-bm_series).sum() 
                ex_annual_return_ratio =  (series-bm_series).sum() * 242 / trading_days 
                sharp_ratio = empyrical.sharpe_ratio(series, 0.035/242)
                return_volatility = empyrical.annual_volatility(series)
                max_drawdown = empyrical.max_drawdown(series)
                information_ratio=series.mean()/series.std()
                win_percent = len(series[series>0]) / trading_days
                ret_3   = series.tail(3).sum()
                ret_10  = series.tail(10).sum()
                ret_21  = series.tail(21).sum()
                ret_63  = series.tail(63).sum()
                ret_126 = series.tail(126).sum()
                ret_252 = series.tail(252).sum()
                return {
                    'return_ratio': return_ratio, 
                    'annual_return_ratio': annual_return_ratio, 
                    'ex_return_ratio': ex_return_ratio, 
                    'ex_annual_return_ratio': ex_annual_return_ratio, 
                    'sharp_ratio': sharp_ratio, 
                    'return_volatility': return_volatility, 
                    'information_ratio':information_ratio, 
                    'max_drawdown': max_drawdown, 
                    'win_percent':win_percent, 
                    'trading_days':trading_days, 
                    'ret_3':ret_3, 
                    'ret_10':ret_10, 
                    'ret_21':ret_21, 
                    'ret_63':ret_63, 
                    'ret_126':ret_126, 
                    'ret_252':ret_252
                    }
            if data_type == 'long':
                perf = cal_stats(self.group_ret[ll_pos], self.group_ret[bm_pos])
            elif data_type =='short':
                perf = cal_stats(self.group_ret[ss_pos], self.group_ret[bm_pos])
            elif data_type =='long_short':
                perf = cal_stats(self.group_ret[ls_pos], self.group_ret[bm_pos])
            return perf
        # 2. IC绩效
        def get_ic(data_type):
            def cal_ic(df):
                return df['daily_ret'].corr(df['factor'], method='spearman')
            def cal_stats(df):
                group_ic_data = df.groupby('date', group_keys=False).apply(lambda x:cal_ic(x)).reset_index()
                ic_data = group_ic_data.rename(columns={0:'g_ic'}).dropna()
                ic_mean = np.nanmean(ic_data['g_ic'])
                ir = np.nanmean(ic_data['g_ic']) / np.nanstd(ic_data['g_ic'])
                ic_3   = ic_data['g_ic'].tail(3).mean()
                ic_10  = ic_data['g_ic'].tail(10).mean()
                ic_21  = ic_data['g_ic'].tail(21).mean()
                ic_63  = ic_data['g_ic'].tail(63).mean()
                ic_126 = ic_data['g_ic'].tail(126).mean()
                ic_252 = ic_data['g_ic'].tail(252).mean()
                return {'ic':ic_mean, 'ir':ir, 'ic_3':ic_3, 'ic_10':ic_10, 'ic_21':ic_21, 'ic_63':ic_63, 'ic_126':ic_126, 'ic_252':ic_252}
            if data_type == 'long':
                ic = cal_stats(self.group_data[self.group_data['group'] == ll_pos][['date','daily_ret','factor']])
            elif data_type == 'short':
                ic = cal_stats(self.group_data[self.group_data['group'] == ss_pos][['date','daily_ret','factor']])
            elif data_type == 'long_short':
                ic = cal_stats(self.group_data[self.group_data['group'].isin([ll_pos, ss_pos])][['date','daily_ret','factor']])
            return ic
        # 3. 周转率
        def get_turnover(data_type):
            def cal_turnover(df):
                def count_repeat(dfs):
                    if dfs.name > 0:
                        return len(set(dfs['instrument']) & set(dfs['instrument_lag']))
                    else:
                        return 0
                df_ins = pd.DataFrame(df.groupby('date').apply(lambda x:x.instrument.tolist()), columns = ['instrument']).reset_index()
                df_ins['instrument_lag'] = df_ins['instrument'].shift(1)
                df_ins['instrument_count'] = df_ins['instrument'].apply(len)
                df_ins['repeat_count'] = df_ins.apply(count_repeat, axis=1)
                df_ins['turnover'] = 1 - df_ins['repeat_count'] / df_ins['instrument_count']
                mean_turnover = np.nanmean(df_ins['turnover'])
                return mean_turnover
            if data_type == 'long':
                turnover = cal_turnover(self.group_data[self.group_data['group'] == ll_pos])
            elif data_type == 'short':
                turnover = cal_turnover(self.group_data[self.group_data['group'] == ss_pos])
            elif data_type == 'long_short':
                turnover = cal_turnover(self.group_data[self.group_data['group'] == ll_pos]) + cal_turnover(self.group_data[self.group_data['group'] == ss_pos])
            return {'turnover':turnover}
        # 三种绩效综合一下
        summary_df = pd.DataFrame() 
        for data_type in ['long', 'short', 'long_short']:
            dict_merged = {} 
            dict1 = get_ic(data_type)
            dict2 = get_basic_perf(data_type)
            dict3 = get_turnover(data_type)
            dict_merged.update(dict1)
            dict_merged.update(dict2)
            dict_merged.update(dict3)
            df = pd.DataFrame.from_dict(dict_merged, orient='index', columns=['value']).T
            df['portfolio'] = data_type 
            summary_df = pd.concat([summary_df,df], axis=0)
        summary_df.index = range(len(summary_df))
        return summary_df

    # （二）多头组合年度收益
    def get_yearly_perf(self):
        # 各投资组合代号
        ll_pos = str(self.params['group_num']-1)
        ss_pos = "0"
        ls_pos = "ls"
        bm_pos = "bm"
        # 计算ic
        def cal_ic(df):
            return df['daily_ret'].corr(df['factor'], method='spearman')
        # 计算收益
        def cal_Performance(df):
            ll_series = df[ll_pos] 
            bm_series = df[bm_pos]
            trading_days = len(ll_series)
            return_ratio =  ll_series.sum() 
            annual_return_ratio = ll_series.sum() * 242 / trading_days 
            ex_return_ratio =  (ll_series-bm_series).sum() 
            ex_annual_return_ratio = (ll_series-bm_series).sum() * 242 / trading_days 
            sharp_ratio = empyrical.sharpe_ratio(ll_series,0.035/242)
            return_volatility = empyrical.annual_volatility(ll_series)
            max_drawdown  = empyrical.max_drawdown(ll_series)
            information_ratio=ll_series.mean()/ll_series.std()
            win_percent = len(ll_series[ll_series>0]) / trading_days
            perf =  pd.DataFrame({
                    'return_ratio': [return_ratio],
                    'annual_return_ratio': [annual_return_ratio],
                    'ex_return_ratio': [ex_return_ratio],
                    'ex_annual_return_ratio': [ex_annual_return_ratio],
                    'sharp_ratio': [sharp_ratio],
                    'return_volatility': [return_volatility],
                    'max_drawdown': [max_drawdown],
                    'win_percent':[win_percent],
                    'trading_days':[int(trading_days)],
                    })
            return perf
        # 计算年度综合收益
        year_df = self.group_ret.reset_index('date')
        year_df['year'] = year_df['date'].apply(lambda x:x.year)
        yearly_perf = year_df.groupby(['year'], group_keys=True).apply(cal_Performance) 
        yearly_perf = yearly_perf.droplevel(1) 
        # 计算年度IC
        group_ic_data = (self.group_data[self.group_data['group'] == ll_pos][['date','daily_ret','factor']]).groupby('date', group_keys=False).apply(lambda x:cal_ic(x)).reset_index()
        ic_data = group_ic_data.rename(columns={0:'g_ic'}).dropna()
        ic_data['year'] = ic_data['date'].apply(lambda x:x.year)
        yearly_ic = ic_data.groupby('year').apply(lambda x:np.nanmean(x['g_ic']))
        yearly_perf['ic'] = yearly_ic
        yearly_perf = yearly_perf.reset_index()
        yearly_perf['year'] = yearly_perf['year'].apply(str) 
        # 返回年度收益
        return yearly_perf

    # （三）全部日期的IC计算
    def get_all_ic(self):
        def cal_ic(df):
            return df['daily_ret'].corr(df['factor'], method='spearman')
        group_ic_data = self.group_data[['date','daily_ret','factor']].groupby('date', group_keys=False).apply(lambda x:cal_ic(x)).reset_index()
        group_ic_data.rename(columns={0:'g_ic'}, inplace=True)
        group_ic_data = group_ic_data.shift(1) 
        group_ic_data['ic_cumsum'] = group_ic_data['g_ic'].cumsum()
        group_ic_data['ic_roll_ma'] = group_ic_data['g_ic'].rolling(22).mean()
        group_ic_data = group_ic_data.dropna()
        return group_ic_data

    # 五、图表展示
    def render(self, local_plot=False):
        # 图表展示因子分析结果
        from bigcharts import opts
        # 1. 表：整体绩效指标
        whole_perf = self.whole_perf[['portfolio','ic', 'ir', 'turnover', 'return_ratio', 'annual_return_ratio','ex_return_ratio', 'ex_annual_return_ratio', 'sharp_ratio', 'return_volatility', 'information_ratio', 'max_drawdown', 'win_percent', 'ic_252', 'ret_252']] 
        c1 = bigcharts.Chart(
            data=whole_perf.round(4),
            type_="table",
            chart_options=dict(title_opts=opts.ComponentTitleOpts(title="整体绩效指标")),
            y=list(whole_perf.columns)
            )
        # 2. 表：年度绩效指标（只考虑多头组合）
        yearly_perf = self.yearly_perf[['year','ic', 'return_ratio', 'annual_return_ratio', 'ex_return_ratio', 'ex_annual_return_ratio', 'sharp_ratio', 'return_volatility', 'max_drawdown', 'win_percent', 'trading_days']]
        c2 = bigcharts.Chart(
            data=yearly_perf.round(4),
            type_="table",
            chart_options=dict(title_opts=opts.ComponentTitleOpts(title="年度绩效指标(多头组合)")),
            y=list(yearly_perf.columns)
            )
        # 3. 图：绘制累积收益图
        c3 = bigcharts.Chart(
            data=self.group_cumret.round(4),
            type_="line",
            x=self.group_cumret.index,
            y=self.group_cumret.columns
            )
        # 4. 表：IC分析指标
        ic = np.nanmean(self.ic['g_ic'])
        ir = np.nanmean(self.ic['g_ic']) / np.nanstd(self.ic['g_ic'])
        abs_ic = self.ic['g_ic'].abs()
        significant_ic_ratio = abs_ic[abs_ic>=0.02].shape[0] / abs_ic.shape[0]
        c4 = bigcharts.Chart(
                data=pd.DataFrame({'IC':[np.round(ic,4)], '|IC|>0.02':[np.round(significant_ic_ratio,4)], 'IR':[np.round(ir,4)]}),
                type_="table",
                chart_options=dict(title_opts=opts.ComponentTitleOpts(title="IC分析指标")),
                y=['IC','|IC|>0.02','IR'],
            )
        # 5. 图：每期IC时序图与绘制IC累计曲线图 
        c5_1 = bigcharts.Chart(
            data=self.ic.round(4),
            type_="bar",
            x='date',
            y=['g_ic', 'ic_roll_ma'],
            chart_options=dict(title_opts=opts.TitleOpts(title="IC曲线",subtitle="每日IC、累计IC、近22日IC均值", pos_left="center",pos_top="top",), legend_opts=opts.LegendOpts(is_show=False,), extend_yaxis=[opts.AxisOpts()])
            )
        c5_2 = bigcharts.Chart(
            data=self.ic.round(4),
            type_="line",
            x='date',
            y=['ic_cumsum'],
            chart_options=dict(title_opts=opts.TitleOpts(title="IC累积曲线",pos_left="center",pos_top="top",), legend_opts=opts.LegendOpts(is_show=False,)), series_options={"ic_cumsum": {"yaxis_index": 1}})
        c5 = bigcharts.Chart(data = [c5_1, c5_2], type_ = "overlap",)
        # 6. 表：展示因子值最大和最小的5只标的
        df_sd = self.factor_data['date'].min().strftime("%Y-%m-%d")
        df_ed = self.factor_data['date'].max().strftime("%Y-%m-%d")
        top_n_ins = 5
        top_factor_df = self.factor_data[self.factor_data['date'] == df_ed] # 最后一天因子数据
        top_factor_df['date'] = top_factor_df['date'].apply(lambda x:x.strftime('%Y-%m-%d'))
        df_sorted_min = top_factor_df.sort_values('factor').head(top_n_ins)
        df_sorted_max = top_factor_df.sort_values('factor', ascending=False).head(top_n_ins)
        c6 = bigcharts.Chart(
            data=df_sorted_max.round(4),
            type_="table",
            chart_options=dict(title_opts=opts.ComponentTitleOpts(title=f"因子值最大的{top_n_ins}只标的")),
            y=['date','instrument','factor'],
            )
        c7 = bigcharts.Chart(
            data=df_sorted_min.round(4),
            type_="table",
            chart_options=dict(title_opts=opts.ComponentTitleOpts(title=f"因子值最小的{top_n_ins}只标的")),
            y=['date','instrument','factor'],
            )
        # 展示所有图表
        c_set = bigcharts.Chart([c1, c2, c3, c4, c5, c6, c7], type_="page").render(display=False)
        from IPython.display import display
        if local_plot:
            display(c_set)
        return c_set.data

    # 六、因子提交
    def submit(self, local_plot=False):

        print("====================因子提交开始====================")

        try:
            perf_df = self.whole_perf  
            perf_dict = {}
            perf_dict['IC'] =  np.nanmean(self.ic['g_ic'])
            perf_dict['IC_cumsum'] = self.ic['ic_cumsum'].iloc[-1]
            perf_dict['long_short_annual_return'] = self.whole_perf['annual_return_ratio'].iloc[-1]

            for i in perf_df.index:
                df = perf_df.iloc[i]
                flag = df['portfolio'] 
                for c in perf_df.columns:
                    perf_dict['%s_%s'%(flag, c)] = df[c]

            perf_dict.update(self.params)
            for key, value in perf_dict.items():
                if isinstance(value, (int, float)):
                    if np.isinf(value) or np.isnan(value):
                        perf_dict[key] = None

            from bigalpha import factors
            report_html = self.render(local_plot=local_plot)
            
            sql_access = f"""
                -- 以下代码为SQL代码，模块化运行时需要在“输入特征（DAI SQL）”中运行，代码数据提取需要以“dai.query(" SQL代码 ")”的形式运行
                SELECT 
                    date, 
                    instrument, 
                    factor 
                FROM {self.params['alpha_id']} 
                ORDER BY date, instrument
            """
            
            # 提交到指定空间
            if self.params['is_bigvip']:
                factors.submit_factor(
                        id = perf_dict['alpha_id'],  #因子分析ID
                        performance_index = perf_dict, # 因子绩效
                        performance_report = report_html, # 因子详情页面
                        metadata = {}, # 可以先不填
                        name = self.params['alpha_name_chinese'], # 中文名 
                        desc = perf_dict['alpha_desc'], 
                        factor_sql = sql_access, 
                        datasource_id = self.params['alpha_id'],
                        is_bigvip = self.params['is_bigvip'],
                        is_featured = self.params['is_featured'],
                        )
            else:
                factors.submit_factor(
                        id = perf_dict['alpha_id'],  #因子分析ID
                        performance_index = perf_dict, # 因子绩效
                        performance_report = report_html, 
                        metadata = {}, # 可以先不填
                        name = self.params['alpha_name_chinese'], 
                        desc = perf_dict['alpha_desc'], 
                        factor_sql = sql_access, 
                        is_bigvip= self.params['is_bigvip'],
                        is_featured = self.params['is_featured'],
                        )
            
            
            print("提交成功：", perf_dict['alpha_id'])

        except  AttributeError as e:
            print('提交失败：', perf_dict['alpha_id'], e)
        
        print("====================因子提交完成====================")

alpha_test = {
    "alpha_class":"test",
    "alpha_name":"teat_0001",
    "alpha_name_chinese":"5日平均收盘价",
    "alpha_sql":"""
        SELECT
            date, 
            instrument, 
            -1 * m_avg(close,5) AS factor
        FROM cn_stock_bar1d
        ORDER BY date, instrument
    """,
    "alpha_desc":" ", 
    "group_num":10, 
    "instruments":"全市场",
    "benchmark":"中证500", 
    "data_process":True,
    "is_bigvip":False,
    "is_featured":False,
}

alpha_miner = AlphaMiner(params=alpha_test)

alpha_miner.render(local_plot=True)

alpha_miner.submit(local_plot=False)