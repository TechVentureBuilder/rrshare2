# coding: utf-8
import pandas as pd
import numpy as np
import os
import time
import datetime
import pymongo
import tushare as ts
from sqlalchemy import create_engine
import psycopg2  
import warnings
warnings.filterwarnings("ignore")

from rrshare.rqUtil.rqLogs import (rq_util_log_debug, rq_util_log_expection,
                                     rq_util_log_info)
from rrshare.rqUtil.rqCode import (rq_util_code_tosrccode, rq_util_code_tostr)
from rrshare.rqUtil.rqDate_trade import rq_util_get_trade_range
from rrshare.rqUtil.rqDate import (rq_util_date_str2int,rq_util_date_int2str)

from rrshare.rqUtil import (PgsqlClass, rq_util_get_last_tradedate)
from rrshare.rqFetch import (pro, fetch_stock_day_adj_fillna_from_tusharepro)
from rrshare.rqFetch.fetch_swl_daily import fetch_swl_daily_tspro_adv

lastTD = rq_util_get_last_tradedate()
print(lastTD)

def create_pgsql(database='rrshare'):
    try:
        PgsqlClass().create_psqlDB(db_name=database)
    except Exception as e:
        print(e)


"""
def rq_util_get_sql_file(path='/home/rome/rrshare/rrshare/sql', file_sql=''):
    file_name_sql = os.path.join(path, file_sql)
    rq_util_log_info(file_name_sql)
    with open(file_name_sql, 'r') as f:
        tsql = f.read()
    return tsql
"""

def create_pgsql_table(tabel_name='', tsql='', db_name='rrshare'):
    try:
        PgsqlClass().create_table(tabel_name, tsql)
    except Exception as e:
        print(e)

def client_pgsql(database='rrshare'):
    try:
        return PgsqlClass().client_pg(db_name=database)
    except Exception as e:
        print(e)

def save_data_to_postgresql(name_biao,data,if_exists='replace',client=client_pgsql()):
        data.to_sql(name_biao,client,index=False,if_exists=if_exists)
    
def load_data_from_postgresql(mes='',client=client_pgsql()):
    res=pd.read_sql(mes,client)
    return res

def read_data_from_pg(table_name='', client=client_pgsql()):
    res = pd.read_sql_table(table_name, client)
    return res

def download_day_data_from_tushare(trade_date='2020010102'):   
    trade_date = trade_date.replace('-', '') #兼容设置以防日期格式为2001-10-20格式
    lastEx = None
    retry = 10
    for _ in range(retry):
        try:
            df_daily=pro.query('daily',trade_date=trade_date)
            df_daily_basic=pro.query('daily_basic',trade_date=trade_date)
            df_factor=pro.query('adj_factor',trade_date=trade_date)             
            break 
        except Exception as ex:
            lastEx = ex
            rq_util_log_info("[{}]TuSharePro数据异常: {}, retrying...".format(trade_date, ex))
        else:
            rq_util_log_info("[{}]TuSharePro异常: {}, retried {} times".format(trade_date,lastEx, retry))
            return None   
    df=pd.merge(df_daily,df_factor,how='left')
    res=pd.merge(df,df_daily_basic,how='left').sort_values(by = 'ts_code')
    res['code']=res['ts_code'].apply(lambda x:x[:6]) #x[7:9].lower()
    res['trade_date'] = pd.to_datetime(res['trade_date'], format='%Y-%m-%d')
    #print(res)
    return res 

def fetch_stock_daily_adj_from_tusharepro(trade_date='20200101'):
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


def rq_fetch_stock_day_pg(code=None,start_date='20190101',end_date='20500118',data='*'):
    name_biao='stock_day'
    if isinstance(code,list):
        code="','".join(code)
        mes='select '+ data+' from '+name_biao+" where  trade_date >= date_trunc('day',timestamp '"+start_date+"') \
                and trade_date <= date_trunc('day',timestamp '"+end_date+"') and code in ('"+code+"')\
                order by trade_date ASC;"
        try:   
            t=time.time()        
            res=load_data_from_postgresql(mes=mes)
            t1=time.time()
            rq_util_log_info('load '+ name_biao+ ' success,take '+str(round(t1-t,2))+' S')              
        except Exception as e:
            print(e)
            res=None
    else:
        rq_util_log_info('code type is not list, please cheack it.')         
    return res

def rq_fetch_stock_list_pg(name_biao='stock_list'):
    mes='select * from '+name_biao+";"    
    try:   
        t=time.time()        
        res=load_data_from_postgresql(mes=mes)
        t1=time.time()
        rq_util_log_info('load '+ name_biao+ ' success,take '+str(round(t1-t,2))+' S')              
    except Exception as e:
        print(e)
        res=None
    return res
  

def rq_save_stock_list_pg():
    stock_list_l= pro.stock_basic(exchange_id='', is_hs='',list_status='L' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    #stock_list_D= pro.stock_basic(exchange_id='', is_hs='',list_status='D' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    #stock_list_P= pro.stock_basic(exchange_id='', is_hs='',list_status='P' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')          
    #stock_list=pd.concat([stock_list_l,stock_list_D],axis=0)
    #stock_list=pd.concat([stock_list_l,stock_list_P],axis=0)
    stock_list = stock_list_l
    stock_list['code']=stock_list['symbol']
    #print(df)
    try:   
        t=time.time()    
        save_data_to_postgresql('stock_list',stock_list)
        t1=time.time()
        rq_util_log_info('save stock_list data success,take '+str(round(t1-t,2))+' S') 
    except Exception as e:        
        print(e) 


def rq_save_stock_day_pg(start_date='20190104'):    
    t = time.localtime(time.time())
    if int(time.strftime('%H%M%S',t))<190000:   #晚上七点之后在更新当天数据，以免不及时
        t = time.localtime(time.time()-3600*24)
        tS = time.strftime("%Y-%m-%d", t)
    else:                
        tS = time.strftime("%Y-%m-%d", t)    
    end_date=tS
    print(end_date)
    try: 
        mes='select distinct trade_date FROM stock_day;'
        trade_data_pg = load_data_from_postgresql(mes).trade_date.tolist()
        print(trade_data_pg[-3:])
        for i in range(len(trade_data_pg)):
            trade_data_pg[i]=trade_data_pg[i].strftime("%Y-%m-%d")
    except: #第一次运行
        trade_data_pg=list()
    print(trade_data_pg[-3:])
    if isinstance(start_date,int):
        start_date=rq_util_date_int2str(start_date)
    elif len(start_date)==8:
        start_date=start_date[0:4]+'-'+start_date[4:6]+'-'+start_date[6:8]
    #print(start_date)
    trade_date=rq_util_get_trade_range(start_date,end_date) 
    #print(trade_date)
    trade_date2=list(set(trade_date).difference(set(trade_data_pg))) #差集
    trade_date2.sort()
    print(trade_date2)
    if len(trade_date2)==0:
        rq_util_log_info('Stock day is up to date and does not need to be updated')
    for i in trade_date2:
        print(i)
        try:
            t=time.time()        
            df=download_day_data_from_tushare(i)    
            #i=i[7:10].lower()+i[0:6]
            save_data_to_postgresql('stock_day',df,'append')
            t1=time.time()   
            rq_util_log_info('save '+i+' stock day success,take '+str(round(t1-t,2))+' S')        
        except Exception as e:
            print(e)


def rq_save_stock_day_adj_fillna_pg(start_date='20190101'): #20050101
    table_name='stock_day_adj_fillna'    
    t = time.localtime(time.time())
    if int(time.strftime('%H%M%S',t))<180000:   #晚上6点之后在更新当天数据，以免不及时
        t = time.localtime(time.time()-3600*24)
        tS = time.strftime("%Y-%m-%d", t)
    else:                
        tS = time.strftime("%Y-%m-%d", t)    
    end_date=tS
    print(end_date)
    try: 
        mes=f'select distinct trade_date FROM {table_name};'
        trade_data_pg = load_data_from_postgresql(mes).trade_date.tolist()
        #print(trade_data_pg[-3:])
        for i in range(len(trade_data_pg)):
            trade_data_pg[i]=trade_data_pg[i].strftime("%Y-%m-%d")
    except: #第一次运行
        trade_data_pg=list()
    #print(trade_data_pg[-3:])
    if isinstance(start_date,int):
        start_date=rq_util_date_int2str(start_date)
    elif len(start_date)==8:
        start_date=start_date[0:4]+'-'+start_date[4:6]+'-'+start_date[6:8]
    #print(start_date)
    trade_date=rq_util_get_trade_range(start_date,end_date) 
    #print(trade_date)
    trade_date2=list(set(trade_date).difference(set(trade_data_pg))) #差集 在trade_date 中 不在trade_data_pg
    trade_date2.sort()
    print(trade_date2)
    if len(trade_date2)==0:
        rq_util_log_info('Stock day adj is up to date and does not need to be updated')
    for i in trade_date2:
        print(i)
        try:
            t=time.time()        
            df=fetch_stock_day_adj_fillna_from_tusharepro(i)    
            #i=i[7:10].lower()+i[0:6]
            save_data_to_postgresql(table_name, df, 'append')
            t1=time.time()
            tt = round((t1-t),4)
            time.sleep(4)
            rq_util_log_info(f'save {i} {table_name} success,take {tt}S')        
        except Exception as e:
            print(e)


def rq_save_swl_day_pg(start_date='2020-01-01'): #from 2015-01-04
    table_name='swl_day'
    t = time.localtime(time.time())
    if int(time.strftime('%H%M%S',t))<210000:   #晚上9点之后在更新当天数据，以免不及时
        t = time.localtime(time.time()-3600*24)
        tS = time.strftime("%Y-%m-%d", t)
    else:
        tS = time.strftime("%Y-%m-%d", t)
    end_date=tS
    print(end_date)
    try:
        mes=f'select distinct trade_date FROM {table_name};'
        #mes='SELECT DISTINCT trade_date FROM swl_day;'
        trade_data_pg = load_data_from_postgresql(mes).trade_date.tolist()
        #print(trade_data_pg[-3:])
        for i in range(len(trade_data_pg)):
            trade_data_pg[i]=trade_data_pg[i].strftime("%Y-%m-%d")
            #trade_data_pg = list(map(lambda x: x.strftime('%Y-%m-%d'), trade_data_pg))
            #print(len(trade_data_pg))
            #print(trade_data_pg[-1:])
    except: #第一次运行
        trade_data_pg=list()
        #print(trade_data_pg[-3:])
    if isinstance(start_date,int):
        start_date=rq_util_date_int2str(start_date)
    elif len(start_date)==8:
        start_date=start_date[0:4]+'-'+start_date[4:6]+'-'+start_date[6:8]
        #print(start_date)
    trade_date=rq_util_get_trade_range(start_date,end_date)
    #print(trade_date)
    trade_date2=list(set(trade_date).difference(set(trade_data_pg))) #差集 在trade_date 中 不在trade_data_pg
    trade_date2.sort()
    print(trade_date2)

    if len(trade_date2)==0:
        rq_util_log_info('swl day is up to date and does not need to be updated')
    for i in trade_date2:
        print(i)
        try:
            t=time.time()
            df=fetch_swl_daily_tspro_adv(trade_date=i)
            save_data_to_postgresql(table_name, df, 'append')
            t1=time.time()
            tt = round((t1-t),4)
            time.sleep(1)
            rq_util_log_info(f'save {i} {table_name} success,take {tt}S')
        except Exception as e:
            print(e)


if __name__ == '__main__':          
    
    rq_save_stock_list_pg()
    rq_save_swl_day_pg('20200101')

    #储存日线数据  包含ts_pro中"daily" "daily_basic" "adj_factor"所有内容 
    #由于采用日期对比机制进行储存，可以增量储存之前数据
    #rq_save_stock_day_pg('20180101')  #rq_save_stock_day_pg('20180101')储存起始日期
    rq_save_stock_day_adj_fillna_pg('20200101')
        
    pass
