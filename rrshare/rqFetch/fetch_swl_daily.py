import pandas as pd

from rrshare.rqUtil import (rq_util_date_str2int, rq_util_get_trade_range, rq_util_get_last_tradedate)
from rrshare.rqFetch import  pro
from rrshare.rqUtil import setting
from rrshare.rqUtil import (rq_util_code_tosrccode,rq_util_code_tostr)
from rrshare.rqUtil import read_data_from_pg, client_pgsql


def fetch_swl_daily_tspro(trade_date=""):
    """ 无trade_date输入；
        如果end_date不给值，为最后交易日
        swl_daily 数据比stock_daily要晚，晚上10以后？？
    """
    trade_date = rq_util_date_str2int(trade_date) if trade_date else rq_util_date_str2int(rq_util_get_last_tradedate())
    print(trade_date) 
    swl_day = pro.sw_daily(trade_date=trade_date)
    swl_day['index'] = swl_day['ts_code'].map(lambda x: x.split(".")[0])
    return swl_day
    

def fetch_swl_daily_tspro_adv(trade_date=''):
    df_swl = read_data_from_pg(table_name='swl_list', client=client_pgsql('rrshare'))
    df_swl_day = fetch_swl_daily_tspro(trade_date)
    df_swl_day_tspro = df_swl.merge(df_swl_day, how='outer', on='index')
    df_swl_day_tspro.drop(columns=['industry_name','ts_code','name'], inplace=True)
    #df_swl_day_tspro.rename(columns={'ts_code':'index_code'}, inplace=True)
    df_swl_day_tspro['trade_date'] = pd.to_datetime(df_swl_day_tspro['trade_date'], format='%Y-%m-%d')
    #df_swl_day_tspro  = df_swl_day_tspro[['trade_date', 'index','name','level','ts_code','industry_name',\
    #      'open','low','high','close','change','pct_change','vol','amount','pe','pb']] 
    #print(df_swl_day_tspro)
    return df_swl_day_tspro
    

if __name__ == '__main__':
    
    #print(pro.sw_daily(trade_date="20210401"))
    #print(fetch_swl_daily_tspro(trade_date='20210506'))
    print(fetch_swl_daily_tspro_adv())
    
    pass

    

