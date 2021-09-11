#coding: utf-8
'''
功能：计算swl（L1，L2,L3）行业指数的不同时段（periods =[5,20,60,120,250]）的回报率rt和PRS
依赖：swl的各行业指数的日行情，tusharepro，->swl_day, swl_valuation
数据库： postgresql12
tips: df.round(2)  ;  sql-> "".jion(list), rq_util_get_pre_trade_date ; rq_util_get_last_tradedate
logging replace print
swl_day, list from rrshare , swl_rs_valuation save to rrfactor
'''
import time
import pandas as pd

import warnings
warnings.filterwarnings("ignore")
import logging
logging.basicConfig(level=logging.INFO, format=' %(asctime)s- %(levelname)s-%(message)s')

from rrshare.rqUtil import (rq_util_log_debug, rq_util_log_expection, rq_util_log_info)
from rrshare.rqUtil import (rq_util_date_today, trade_date_sse,rq_util_get_trade_range,
                            rq_util_get_last_tradedate, rq_util_get_pre_trade_date)
from rrshare.rqUtil import (PERIODS, is_trade_time_secs_cn)
from rrshare.rqUtil.rqPgsql import (PgsqlClass, client_pgsql, read_data_from_pg, read_table_from_pg,read_sql_from_pg, save_data_to_postgresql)
from rrshare.rqFetch.rqCodeName import (stock_code_to_name, stock_code_belong_swl_name, swl_index_to_name)
from rrshare.rqFetch import Swsindex
from rrshare.rqUtil import (PERIODS , SWL_LEVEL)


L = SWL_LEVEL().LEVEL
periods = PERIODS().PERIODS

NList = PERIODS().PERIODS
N1List = list(map(lambda x: x-1,NList))
client_rrshare = client_pgsql('rrshare')
client_rrfactor = client_pgsql('rrfactor')


class SwlPRS(object):
    
    def __init__(self, level):
        self.level = level

    def swl_PRS_level(self, period):
        cols = "trade_date, index, name_level, level, pct_change, pe, pb"
        start_date = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(), period)
        df = read_sql_from_pg(start_date=start_date, data=cols, table_name='swl_day',client=client_rrshare)
        df = df[df['level']==self.level]
        df = df[['index','name_level', 'pct_change']]
                
        rt = df.groupby(by='index').sum()
        rt['rank'] = rt['pct_change'].rank(ascending=False)
        rt['rs'] = round(100*(1 - rt['rank']/len(rt)), 2)
        rt = rt.rename(columns={'pct_change': f'rt_{str(period)}'})
        rt = rt.rename(columns={'rs': f'rs_{str(period)}','rank':f'rank_{str(period)}'})
        rt =rt.round(2)
        #print(rt)
        df_name = swl_index_to_name(swl_level=self.level).set_index('index', drop=False)  # keep index in columns
        #df_name['name'] = df_name['name_level']
        #print(df_name)
        RS = pd.concat([df_name, rt], axis=1, sort=True)
        RS = RS[['index','name_level', f'rt_{str(period)}',f'rs_{str(period)}']]
        logging.info(f'\n {RS}')
        #print(RS)
        return RS


    def swl_Prs_all_periods(self):
        RS = pd.DataFrame()
        for period in periods:
            rs = self.swl_PRS_level(period)
            RS = pd.concat([RS, rs], axis=1, sort=True)
        #print(RS)
        save_data_to_postgresql(RS,f'swl_rs_{self.level}',client_rrfactor)
        logging.info(f'写入数据库的表swl_rs_{self.level}, ok')


    def swl_rt_all_periods(self):
        RT = pd.DataFrame()
        level1 = L[0]
        for period in periods:
            p = period -1
            cols = "trade_date, index, name_level, level, pct_change, pe, pb"
            start_date = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(), p)
            df = read_sql_from_pg(start_date=start_date, data=cols, table_name='swl_day',client=client_rrshare)
            df = df[df['level']==level1]
            df = df[['index','name_level', 'pct_change']]
            rt = df.groupby(by='index').sum()
            rt = rt.rename(columns={'pct_change': f'rt_{str(p)}'})
            rt =rt.round(2)
            #print(rt)
            RT = pd.concat([RT, rt], axis=1, sort=True)
        
        df_name = swl_index_to_name(swl_level=level1).set_index('index', drop=False)  # keep index in columns
        #df_name['name'] = df_name['name_level']
        print(df_name)
        RT = pd.concat([df_name, RT], axis=1, sort=True)
        logging.info(f'\n {RT}')
        #print(RT)
        save_data_to_postgresql(RT,f'swl_rt_{level1}',client_rrfactor)
        logging.info(f'写入数据库的表swl_rt_{level1}, ok')
        
        return RT


    def swl_valuation(self):
        df = read_table_from_pg(period=1,table_name='swl_day', client=client_rrshare)
        df = df[['trade_date', 'index', 'name_level','level','close','pct_change', 'pe', 'pb']]
        df = df[df['level']==self.level]
        df = df.set_index('index', drop=False)  # keep index in columns
        df = df.sort_values(by='pct_change', ascending=False)
        logging.info(f'\n {df}')
        return df


    def save_swl_valuation_pg(self):
        df =self.swl_valuation()
        save_data_to_postgresql(df, f'swl_valuation_{self.level}', client_rrfactor)
        logging.info(f'写入数据库的表swl_valuation_{self.level}, ok')


    def swl_rs_valuation(self):
        RS = read_data_from_pg(table_name=f'swl_rs_{self.level}',client=client_rrfactor)
        rs_cols = [f'rs_{x}' for x in periods]
        cols = ['index','name_level'] + rs_cols 
        RS = RS[cols]
        logging.info(RS)
        valuation = read_data_from_pg(f'swl_valuation_{self.level}', client_rrfactor)
        df = pd.merge(RS, valuation , on='index')
        logging.info(f'\n {df}')
        return df


    def save_swl_rs_valuation_pg(self):
        df =self.swl_rs_valuation()
        save_data_to_postgresql(df, f'swl_rs_valuation_{self.level}', client_rrfactor)
        logging.info(f'写入数据库的表swl_rs_valuation_{self.level}, ok')


def swl1_index_realtime_prs():
    level1=L[0]
    rt = read_data_from_pg(f'swl_rt_L1', client_rrfactor)
    #print(rt)
    new_rt  = Swsindex().get_swsindex_L1_realtime()[['index','pct_change','trade_date']] 
    print(new_rt)
    cols_1 = [ 'rt_'+ str(int(x -1)) for x in periods] 
    cols = ['rt_' + str(x) for x in periods]
    col_dict = dict(zip(cols,cols_1))
    #print(cols_1,cols)
    df = pd.merge(rt, new_rt, on='index', how='right' )
    for k, v in col_dict.items():
        df[k] = df[v] + df['pct_change']
    
    col_rs = ['rs_5','rs_10','rs_20','rs_60','rs_120', 'rs_250']
    col_rs_dict = dict(zip(col_rs, cols))
    #print(col_rs_dict)
    for k, v in col_rs_dict.items():
        df[k] = 100*df[v].rank(axis=0,ascending=True, pct=True)

    cols_prs = ['index','name_level','trade_date', 'pct_change',\
                'rs_5','rs_10','rs_20','rs_60','rs_120', 'rs_250',\
               ]
    df = df[cols_prs].round(2)
    df.sort_values(by='pct_change', ascending=False, inplace=True)
    save_data_to_postgresql(df, f'swl1_rs_{level1}', client_rrfactor)
    logging.info(f"\n {df}")
    return df


def update_swl_rs_valuation(L=L):
    for l in L:
        prs = SwlPRS(l)
        prs.swl_Prs_all_periods()
        prs.save_swl_valuation_pg()
        prs.save_swl_rs_valuation_pg()


def update_swl_all_main():
    prs = SwlPRS("L1")
    SwlPRS('L1').swl_rt_all_periods()

    prs.swl_PRS_level(period=20)
    prs.swl_Prs_all_periods()
    prs.swl_valuation()
    update_swl_rs_valuation(L)


if __name__ == '__main__':
    
    update_swl_all_main()
    pass
