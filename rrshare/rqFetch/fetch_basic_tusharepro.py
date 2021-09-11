# coding=utf8
import json
import pandas as pd
import tushare as ts

from rrshare.rqUtil import rq_util_get_last_tradedate
from rrshare.rqFetch import pro

def fetch_delist_stock(trade_date=None):
    df_P = pro.stock_basic(exchange='',list_status='P',fields='ts_code,symbol,name,list_status,delist_date,list_date')
    df_D = pro.stock_basic(exchange='',list_status='D',fields='ts_code,symbol,name,list_status,delist_date,list_date')
    df_DP = pd.concat([df_P, df_D], axis=0)
    df_DP['code'] = df_DP['ts_code'].apply(lambda x: x[0:6])
    if not trade_date:
        trade_date = rq_util_get_last_tradedate().replace('-','')
    df_DD = df_DP[df_DP['delist_date'] <= trade_date]
    df_DD_code = list(df_DD['code'].values)
    return df_DD_code

def fetch_stock_list_tusharepro():    
    stock_list_l= pro.stock_basic(exchange_id='', is_hs='',list_status='L' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    stock_list_D= pro.stock_basic(exchange_id='', is_hs='',list_status='D' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    stock_list_P= pro.stock_basic(exchange_id='', is_hs='',list_status='P' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')          
    stock_list=pd.concat([stock_list_l,stock_list_D],axis=0)
    stock_list=pd.concat([stock_list,stock_list_P],axis=0)
    return stock_list

def fetch_get_stock_list(trade_date=None,ts_code=True):
    df_L= pro.stock_basic(exchange_id='', is_hs='',list_status='L') # , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    #stock_list_P= pro.stock_basic(exchange_id='', is_hs='',list_status='P' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')          
    #stock_list=pd.concat([stock_list_l,stock_list_P],axis=0)
    if ts_code:
        return list(df_L['ts_code'].values)
    df_L['code'] = df_L['ts_code'].apply(lambda x: x[0:6])
    #print(df_L)
    return list(df_L['code'].values)

def fetch_get_stock_list_adj(trade_date=None, ts_code=False):
    if not trade_date:
        trade_date = rq_util_get_last_tradedate().replace('-','')
    adj = pro.adj_factor(trade_date=trade_date)
    if ts_code:
        return list(adj['ts_code'].values)
    adj['code'] = adj['ts_code'].apply(lambda x: x[0:6])
    return list(adj['code'].values)


def fetch_pro_bar(ts_code=None,asset="E", adj='qfq', start_date='20180101', end_date='20181011'):
    df = ts.pro_bar(ts_code=ts_code, asset=asset,adj=adj, start_date=start_date, end_date=end_date) 
    return df


if __name__ == '__main__':
    
    #print(fetch_delist_stock())
    print(len(fetch_get_stock_list()), fetch_get_stock_list(ts_code=True)[0:6])
    print(len(fetch_get_stock_list_adj()), fetch_get_stock_list_adj(ts_code=True)[-5:])
    print(fetch_pro_bar(ts_code='000001.SH',asset="I", adj='qfq', start_date='20210101', end_date='20210507'))
    pass
