# coding: utf-8
import pandas as pd
import numpy as np
import time
import datetime
import tushare as ts
import warnings
warnings.filterwarnings("ignore")

from rrshare.rqUtil.rqLogs import (rq_util_log_debug, rq_util_log_expection,
                                     rq_util_log_info)
from rrshare.rqUtil.rqCode import (rq_util_code_tosrccode, rq_util_code_tostr)
from rrshare.rqUtil.rqDate_trade import rq_util_get_trade_range
from rrshare.rqUtil.rqDate import (rq_util_date_str2int,rq_util_date_int2str)

from rrshare.rqUtil import setting, PERIODS

PERIODS = PERIODS().PERIODS
token= setting['TSPRO_TOKEN']

try:
    ts.set_token(token)
    pro=ts.pro_api()
except Exception as e:
    print(e)


def fetch_stock_bar_from_tusharepro(trade_date='20190102'):   
    trade_date = trade_date.replace('-', '') #兼容设置以防日期格式为2001-10-20格式
    lastEx = None
    retry = 5
    for _ in range(retry):
        try:
            df_daily=pro.query('daily',trade_date=trade_date)
            #print(df_daily)
            df_factor=pro.query('adj_factor',trade_date=trade_date)             
            #print(df_factor)
            break 
        except Exception as ex:
            lastEx = ex
            rq_util_log_info("[{}]TuSharePro数据异常: {}, retrying...".format(trade_date, ex))
    else:
        rq_util_log_info("[{}]TuSharePro异常: {}, retried {} times".format(trade_date,lastEx, retry))
        return None   
    df=pd.merge(df_daily,df_factor,how='left')
    df['code']=df['ts_code'].apply(lambda x:x[:6]) #x[7:9].lower()
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')
    df['vwap'] = 10*df['amount'] / df['vol']
    for col in ['close','open','high','low','pre_close','vwap']:
        df[col] = df[col] * df['adj_factor']
        #df['pct_change'] = 100*(df['close']-df['pre_close'])/df['pre_close']
        df[col] = df[col].map(lambda x: "{:+.2f}".format(x)).astype(float)
    #print(df[df.code=='600519'])
    return df 


def fetch_stock_day_adj_fillna_from_tusharepro(trade_date='20050104'):
    trade_date = trade_date.replace('-', '') #兼容设置以防日期格式为2001-10-20格式
    lastEx = None
    retry = 10
    for _ in range(retry):
        try:
            df_daily=pro.query('daily',trade_date=trade_date)
            #df_daily_basic=pro.query('daily_basic',trade_date=trade_date)
            df_factor=pro.query('adj_factor',trade_date=trade_date)             
            break 
        except Exception as ex:
            lastEx = ex
            rq_util_log_info("[{}]TuSharePro数据异常: {}, retrying...".format(trade_date, ex))
        else:
            rq_util_log_info("[{}]TuSharePro异常: {}, retried {} times".format(trade_date,lastEx, retry))
            return None   
    #df=pd.merge(df_daily,df_factor,how='left')
    res=pd.merge(df_factor,df_daily,how='left').sort_values(by = 'ts_code')
    res.fillna({'change':0,'pct_chg':0,'vol':0,'amount':0}, inplace=True)
    res['code']=res['ts_code'].apply(lambda x:x[:6]) #x[7:9].lower()
    res['trade_date'] = pd.to_datetime(res['trade_date'], format='%Y-%m-%d')
    #print(res)
    return res


def fetch_stock_list_tusharepro():    
    stock_list_l= pro.stock_basic(exchange_id='', is_hs='',list_status='L' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    stock_list_D= pro.stock_basic(exchange_id='', is_hs='',list_status='D' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    stock_list_P= pro.stock_basic(exchange_id='', is_hs='',list_status='P' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')          
    stock_list=pd.concat([stock_list_l,stock_list_D],axis=0)
    stock_list=pd.concat([stock_list,stock_list_P],axis=0)
    return stock_list

def fetch_get_stock_day_hfq_one(name, start='', end='', adj='hfq', freq='D', ma=PERIODS,adjfactor=True, type_='pd'):
    """在Tushare数据接口里，不管是旧版的一些接口还是Pro版的行情接口，都是以用户设定的end_date开始往前复权，跟所有行情软件或者财经网站上看到的前复权可能存在差异，
    因为所有这些都是以最近一个交易日开始往前复权的。比如今天是2018年10月26日，您想查2018年1月5日～2018年9月28日的前复权数据，Tushare是先查找9月28日的复权因子，
    从28日开始复权，而行情软件是从10月26日这天开始复权的。
    """
    def fetch_data():
        data = None
        try:
            time.sleep(0.002)
            #pro = get_pro()
            data = ts.pro_bar(
                api=pro,
                ts_code=str(name),
                asset='E',
                adj=adj,
                start_date=start,
                end_date=end,
                ma=ma,
                freq=freq,
                adjfactor=adjfactor,
                factors=['tor',
                         'vr']
            ).sort_index()
            print('fetch done: ' + str(name))
        except Exception as e:
            print(e)
            print('except when fetch data of ' + str(name))
            time.sleep(1)
            data = fetch_data()
        return data
    data = fetch_data()

    #data['date_stamp'] = data['trade_date'].apply(lambda x: cover_time(x))
    data['code'] = data['ts_code'].apply(lambda x: str(x)[0:6])
    data['fqtype'] = adj
    if type_ in ['json']:
        data_json = QA_util_to_json_from_pandas(data)
        return data_json
    elif type_ in ['pd', 'pandas']:
        data['date'] = pd.to_datetime(data['trade_date'], format='%Y%m%d')
        data = data.set_index('date', drop=False)
        data['date'] = data['date'].apply(lambda x: str(x)[0:10])
        return data


if __name__ == '__main__':          
    print(fetch_stock_list_tusharepro())
    print(fetch_stock_day_adj_fillna_from_tusharepro('2021-02-10'))
    #print(fetch_get_stock_day_hfq_one(name='600519.SH', start='20210101',end='20210212'))
    pass


