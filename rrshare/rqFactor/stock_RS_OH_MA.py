"""
交易时间：：使用——pre + realtime
历史数据更新后： 使用last

功能：计算stock_day_hfq不同时段（periods =[5,20,60,120,250]）的回报率rt和PRS
依赖：stock_day_hfq的日行情->rrshare(pg),stock_day_hfq
数据库： postgresql12
tips: df.round(2)  ;  sql-> "".jion(list), rq_util_get_pre_trade_date ; rq_util_get_last_tradedate
logging replace print
# pd.concat() sort=true -- wrong : update jqdatasdk(envs:jqdata)
"""
import time
import datetime
import pickle
import pandas as pd
import numpy as np
from decimal import Decimal
import warnings
warnings.filterwarnings("ignore")
import logging
logging.basicConfig(level=logging.INFO, format=' %(asctime)s- %(levelname)s-%(message)s')

from rrshare.rqUtil import (rq_util_log_debug, rq_util_log_expection,
                                                     rq_util_log_info)
from rrshare.rqUtil import (rq_util_date_today, trade_date_sse,rq_util_get_trade_range,
                            rq_util_get_last_tradedate, rq_util_get_pre_trade_date)
from rrshare.rqUtil import (PERIODS, is_trade_time_secs_cn)
from rrshare.rqUtil import (client_pgsql, read_data_from_pg, read_sql_from_pg)
from rrshare.rqFetch import (stock_code_to_name, fetch_delist_stock, fetch_realtime_price_stock_day_adj)

NList = PERIODS().PERIODS
N1List = list(map(lambda x: x-1,NList))
client_rrshare = client_pgsql('rrshare')
client_rrfactor = client_pgsql('rrfactor')

def stock_RT_HH_MA_pre(table_name='stock_day_adj_fillna', N=250):
    """caculate i-1 day rt_ma
       prepare to next day  day rs_OH_ma
       迭代， 只算一次， 速度快。
    """
    trade_date = rq_util_get_last_tradedate()
    #print(trade_date)
    start_date = rq_util_get_pre_trade_date(trade_date,N)
    df = read_sql_from_pg(start_date=start_date, data=\
            "trade_date,open,high,low,close,pre_close,vol,amount,\
            adj_factor,code,pct_chg,avg",table_name=table_name, client=client_rrshare)
    delist_code = fetch_delist_stock(trade_date) 
    #df = pd.read_pickle('/home/rome/.rrshare/data/stock_day_adj_250.pkl')
    df =df[~df['code'].isin(delist_code)]
    df_i = df.set_index(['trade_date','code'])
    df_i.drop_duplicates(keep='last',inplace=True)
    df_un = df_i.unstack()
    df_un_fill = df_un.fillna(method='ffill')
    vol_mean =  (df_un_fill['vol']*df_un_fill['adj_factor']).rolling(39).mean().unstack()
    vol_chg = 100*((df_un_fill['vol']*df_un_fill['adj_factor']) / \
            (df_un_fill['vol']*df_un_fill['adj_factor']).rolling(50).mean() - 1).unstack()
    df_fill = df_un_fill.stack().reset_index()
    ma=dict()
    rt=dict()
    #rs=dict()
    for i in N1List:
        ma[i] = ((df_un_fill['close']*df_un_fill['adj_factor']).rolling(i).mean()/df_un_fill['adj_factor']).unstack()
        rt[i] = df_un_fill['pct_chg'].rolling(i).sum().unstack()
        #rs[i]= 100*rt[i].unstack(0).rank(axis=1,ascending=True, pct=True).unstack()
    H = (df_un_fill['high']*df_un_fill['adj_factor']).expanding().max()
    HH = (H/df_un_fill['adj_factor']).unstack()
    L = (df_un_fill['low']*df_un_fill['adj_factor']).expanding().min()
    LL = (L/df_un_fill['adj_factor']).unstack()
    
    close = df_un_fill['close'].unstack()
    pct_chg = df_un_fill['pct_chg'].unstack()
    adj_factor = df_un['adj_factor'].unstack()

    pct_ma_rs = pd.DataFrame({
    'close_pre':close, 'pch_chg_pre': pct_chg, 'adj_factor_pre':adj_factor, 'vol_mean':vol_mean, 'vol_chg':vol_chg,
    'ma4':ma[4],'ma9':ma[9],'ma19':ma[19], 'ma59':ma[59],'ma119':ma[119],'ma249':ma[249],
    'rt4':rt[4],'rt9':rt[9],'rt19':rt[19],'rt59':rt[59],'rt119':rt[119], 'rt249': rt[249], 
    'H':HH,'L':LL, 
    })

    data = pct_ma_rs.reset_index()
    data = data[data['trade_date'] == trade_date]
    data.dropna(subset=['adj_factor_pre','close_pre'],inplace=True)
    logging.info(len(data))
    logging.info(f'\n {data.head()}')
    #  20:00 after  market close on tradedate --> change
    #data.to_pickle('/home/rome/.rrshare/data/stock_RT_HH_MA_pre.pkl')
    save_PRS_to_pg(table_name="stock_RT_HH_MA_pre",data=data,client=client_rrfactor)
    return data 


def stock_RT_HH_MA_last(table_name='stock_day_adj_fillna', N=250):
    """caculate i day rt_hh_ma
       prepare to next day  day rs_OH_ma
       迭代， 只算一次， 速度快。
    """
    trade_date = rq_util_get_last_tradedate()
    #print(trade_date)
    start_date = rq_util_get_pre_trade_date(trade_date,N)
    df = read_sql_from_pg(start_date=start_date, data=\
            "trade_date,open,high,low,close,pre_close,vol,amount,\
            adj_factor,code,pct_chg,avg",table_name=table_name, client=client_rrshare)
    delist_code = fetch_delist_stock(trade_date) 
    #df = pd.read_pickle('/home/rome/.rrshare/data/stock_day_adj_250.pkl')
    df =df[~df['code'].isin(delist_code)]
    df_i = df.set_index(['trade_date','code'])
    df_i.drop_duplicates(keep='last',inplace=True)
    df_un = df_i.unstack()
    df_un_fill = df_un.fillna(method='ffill')
    #vol_mean =  (df_un_fill['vol']*df_un_fill['adj_factor']).rolling(39).mean().unstack()
    vol_chg = 100*((df_un_fill['vol']*df_un_fill['adj_factor']) / \
            (df_un_fill['vol']*df_un_fill['adj_factor']).rolling(50).mean() - 1).unstack()
    df_fill = df_un_fill.stack().reset_index()
    ma=dict()
    rt=dict()
    #rs=dict()
    for i in NList:
        ma[i] = ((df_un_fill['close']*df_un_fill['adj_factor']).rolling(i).mean()/df_un_fill['adj_factor']).unstack()
        rt[i] = df_un_fill['pct_chg'].rolling(i).sum().unstack()
        #rs[i]= 100*rt[i].unstack(0).rank(axis=1,ascending=True, pct=True).unstack()
    H = (df_un_fill['high']*df_un_fill['adj_factor']).expanding().max()
    HH = (H/df_un_fill['adj_factor']).unstack()
    L = (df_un_fill['low']*df_un_fill['adj_factor']).expanding().min()
    LL = (L/df_un_fill['adj_factor']).unstack()
    
    close = df_un_fill['close'].unstack()
    pct_chg = df_un_fill['pct_chg'].unstack()
    adj_factor = df_un['adj_factor'].unstack()

    pct_ma_rs = pd.DataFrame({
    'close':close,  'adj_factor':adj_factor,  'pct_chg_pre':pct_chg,
    'vol_chg':vol_chg,
    'ma5':ma[5],'ma10':ma[10],'ma20':ma[20], 'ma60':ma[60],'ma120':ma[120],'ma250':ma[250],
    'rt5':rt[5],'rt10':rt[10],'rt20':rt[20],'rt60':rt[60],'rt120':rt[120], 'rt250': rt[250], 
    'H':HH,'L':LL, 
    })
    
    data = pct_ma_rs.reset_index()
    data.dropna(subset=['adj_factor','close'], inplace=True)
    data = data[data['trade_date'] == trade_date]
    logging.info(len(data))
    logging.info(f'\n {data.head()}')
    #data.to_pickle('/home/rome/.rrshare/data/stock_RT_HH_MA.pkl')
    save_PRS_to_pg(table_name="stock_RT_HH_MA",data=data,client=client_rrfactor)
    return data 


def stock_RS_OH_MA_new():
    """ fast and right pkl or h5 or sql
    """
    #df = pd.read_pickle('/home/rome/.rrshare/data/stock_RT_HH_MA_pre.pkl')
    df = pd.read_sql_table("stock_RT_HH_MA_pre", client_rrfactor)
    logging.info(df.head())

    df_last_date =str( max(set(list(df['trade_date'].values))))[0:10]
    print(df_last_date)

    today_ = rq_util_date_today().strftime('%Y-%m-%d') 
    trade_date = today_  if today_ in trade_date_sse else rq_util_get_last_tradedate()
    delist_code = fetch_delist_stock(trade_date) 
    df =df[~df['code'].isin(delist_code)]
    print(trade_date)

      
    if (trade_date > df_last_date): 
        #fetch_realtime_price from rqFetch
        df_p = fetch_realtime_price_stock_day_adj()
        df_p = df_p.drop(columns =['ts_code', 'trade_date','amount','open'], axis=1)
        logging.info(df_p.head())
        #concat
        df = pd.merge(df,df_p, how='left', on='code')
        #print(df)
        #vol_chg = 100*((df['vol']*df['adj_factor']) / (df['vol']*df['adj_factor']).rolling(50).mean() - 1)
        col_ma_pre = ['ma4','ma9', 'ma19', 'ma59', 'ma119', 'ma249']
        col_ma = ['ma5','ma10','ma20', 'ma60','ma120','ma250']
        col_rt_pre =['rt4', 'rt9', 'rt19', 'rt59','rt119', 'rt249']
        col_rt =['rt5', 'rt10', 'rt20', 'rt60','rt120', 'rt250']
        col_ma_dict = dict(zip(col_ma,col_ma_pre))
        print(col_ma_dict)
        col_rt_dict = dict(zip(col_rt, col_rt_pre))
        for  k, v in col_ma_dict.items():
            n = int(k[2:])
            #print(n)
            df[k] = ((n-1)*df[v] + df['close']) / n
            #print(df[k])
            df[k] = df[k].apply(lambda x: Decimal(x).quantize(Decimal(('0.00'))))
        for  k, v in col_rt_dict.items():
            n = int(k[2:])
            df[k] = df[v] + df['pct_chg']
        df.dropna(subset=['H','L','name'],inplace=True)   # TODO ???
        #print(df)
        vol_chg = 100*((df['vol']*df['adj_factor']) / \
            (df['vol']*df['adj_factor']).rolling(50).mean() - 1)  # TODO
        df['H'] = df['H']*df['adj_factor_pre'] / df['adj_factor']
        df['H'] = df.apply(lambda x: max(x.H, x.high), axis=1) 
        df['L'] = df['L']*df['adj_factor_pre'] / df['adj_factor']
        df['L'] = df.apply(lambda x: min(x.L, x.low), axis=1)
        df['OH'] = 100*(df['close']/df['H'])
        #df['OH'] = df['OH'].apply(lambda x: Decimal(x).quantize(Decimal((0.00))))
        df['OL'] = 100*(df['close']/df['L'] - 1)
        #df['OL'] = df['OL'].apply(lambda x: Decimal(x).quantize(Decimal((0.00))))

        #rsi= 100*rt[i].unstack(0).rank(axis=1,ascending=True, pct=True).unstack()
        col_rs = ['rs_5','rs_10','rs_20','rs_60','rs_120', 'rs_250']
        col_rs_dict = dict(zip(col_rs,col_rt))
        for k, v in col_rs_dict.items():
            df[k] = 100*df[v].rank(axis=0,ascending=True, pct=True)

        cols = ['date', 'time', 'code', 'close', 'adj_factor', 'pct_chg',\
                'ma5','ma10','ma20', 'ma60', 'ma120','ma250',\
                'rs_5','rs_10','rs_20','rs_60','rs_120', 'rs_250',\
                'rt5', 'rt10', 'rt20', 'rt60','rt120', 'rt250',\
                'H','L','OH','OL']

        cols_old = ['trade_date', 'code','close', 'adj_factor','pct_chg',\
                'ma5','ma10','ma20', 'ma60', 'ma120','ma250',\
                'rs_5','rs_10','rs_20','rs_60','rs_120', 'rs_250',\
                'rt5', 'rt10', 'rt20', 'rt60','rt120', 'rt250',\
                'H','L','OH','OL']
        #print(cols)
        df = df[cols].round(2)

        df_name = read_data_from_pg('stock_belong_swl',client_rrshare)[['code', 'name', 'swl_L1', 'swl_L2','swl_L3']]
        df = pd.merge(df,df_name,how='left', on='code')
        df.dropna(subset=['name'],inplace=True)
        df.rename(columns={'date':'trade_date'}, inplace=True)
        save_PRS_to_pg(table_name="stock_RS_OH_MA_new",data=df, client=client_rrfactor)
        logging.info(f"\n {df.head()}")
        return df
    else:
        df = read_data_from_pg(table_name='stock_RT_HH_MA',client=client_rrfactor)
        logging.info(f'\n {df.head()}')
        df.rename(columns={'close_pre':'close','adj_factor_pre':'adj_factor','pct_chg_pre':'pct_chg'}, inplace=True)
        df['OH'] = 100*(df['close']/df['H'])
        #df['OH'] = df['OH'].apply(lambda x: Decimal(x).quantize(Decimal((0.00))))
        df['OL'] = 100*(df['close']/df['L'] - 1)
        #df['OL'] = df['OL'].apply(lambda x: Decimal(x).quantize(Decimal((0.00))))
        col_rt =['rt5', 'rt10', 'rt20', 'rt60','rt120', 'rt250']
        col_rs = ['rs_5','rs_10','rs_20','rs_60','rs_120', 'rs_250']
        col_rs_dict = dict(zip(col_rs,col_rt))
        for k, v in col_rs_dict.items():
            df[k] = 100*df[v].rank(axis=0,ascending=True, pct=True)

        cols = ['trade_date', 'code','close', 'adj_factor','pct_chg',\
                'ma5','ma10','ma20', 'ma60', 'ma120','ma250',\
                'rs_5','rs_10','rs_20','rs_60','rs_120', 'rs_250',\
                'rt5', 'rt10', 'rt20', 'rt60','rt120', 'rt250',\
                'H','L','OH','OL']
        #print(cols)
        df = df[cols].round(2)
        #print(df)
        df_name = read_data_from_pg('stock_belong_swl', client_rrshare)[['code', 'name', 'swl_L1', 'swl_L2','swl_L3']]
        df = pd.merge(df,df_name,how='left', on='code')
        df.dropna(subset=['name'],inplace=True)
        save_PRS_to_pg(table_name="stock_RS_OH_MA", data=df, client=client_pgsql('rrfactor'))
        logging.info(f'\n {df.head()}')
        return df


def stock_select_PRS_new():
    df  = pd.read_sql_table('stock_RS_OH_MA_new', client_rrfactor) 
    logging.info(df.head())

    df = df[(df['rs_250'] > 50) & (df['rs_120'] > 70) & (df['rs_60'] > 70) &  (df['rs_20']> 60) \
         & (df['rs_10']> 70) & (df['rs_5']> 70 )]

    df = df[ (df['OH'] > 75)  & (df['OL'] > 20)]

    df = df[(df['ma60'] > df['ma250']) & (df['ma20'] > df['ma250']) & (df['ma10'] > df['ma250'])]

    df =  df[(df['ma20'] > df['ma120']) & (df['ma10'] > df['ma120']) ]

    df = df[['trade_date','time','code','close', 'pct_chg',\
                'ma5','ma10','ma20', 'ma60', 'ma120','ma250',\
                'rs_5','rs_10','rs_20','rs_60','rs_120', 'rs_250',\
                #'rt5', 'rt10', 'rt20', 'rt60','rt120', 'rt250',\
                'H','L','OH','OL',\
                'name', 'swl_L1', 'swl_L2','swl_L3']]
    save_PRS_to_pg('stock_select_PRS', df, client_rrfactor)
    logging.info(f'\n {df}')
    return df
   
def save_PRS_to_pg(table_name="",data="", client=client_rrfactor):
    #写入数据，table_name为表名，‘replace’表示如果同名表存在就替换掉
    if isinstance(data, pd.DataFrame):
        try:
            data.to_sql(table_name, client, index=False, if_exists='replace')
            print(f'写入数据库的表{table_name} , ok')
        except Exception as e:
            print(e)

def update_stock_PRS_day():
    stock_RT_HH_MA_pre()
    stock_RT_HH_MA_last()

def update_stock_PRS_new():
    stock_RS_OH_MA_new()
    stock_select_PRS_new()
    pass

if __name__ == '__main__':
    # new -- date , time 
    # day -- trade_date 
    # TODO
    update_stock_PRS_day()
    update_stock_PRS_new()
    #print(is_trade_time_secs_cn())
    pass

