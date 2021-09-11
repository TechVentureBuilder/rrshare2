# coding=utf-8
import time
import pandas as pd
import easyquotation as eq 
from rrshare.rqUtil import rq_util_log_info
#from apscheduler.schedulers.blocking import BlockinfScheduler
#sched = BlockinfScheduler()

#quotation = eq.use('sina')  # 新浪 ['sina'] 腾讯 ['tencent', 'qq'] 
#获取所有股票行情
#snapshot_all = quotation.market_snapshot(prefix=True) 
# # prefix 参数指定返回的行情字典中的股票代码 key 是否带 sz/sh 前缀
#单只股票
#one_dict  = quotation.real('300146')
# 支持直接指定前缀，如 'sh000001'

def task_time(func_args): # TODO
    def in_func(*args, **kwargs):
        while True:
            start_time=time.time()
            data = func_args(*args, **kwargs)
            if isinstance(data, dict):
                df =  pd.DataFrame(data).T.reset_index()
            rq_util_log_info(time.time() - start_time)
            time.sleep(5)
        return df 
    return in_func
    
    
def fetch_realtime_price_all(df=True,src='sina'): # or ['tecent','qq']
    quotation = eq.use(src)
    price_all = quotation.market_snapshot(prefix=True)
    if not df:
        return price_all
    price_df = pd.DataFrame(price_all).T.reset_index()
    return price_df

def fetch_realtime_price_stock(symbols=None,df=True,src='sina'): # or ['tecent','qq']
    quotation = eq.use(src)
    prices = quotation.stocks(symbols)
    if not df:
        return prices
    return pd.DataFrame(prices).T.reset_index()


def fetch_snapshot_timer():
    #quotation = eq.use('sina')
    while True:
        df = fetch_realtime_price_all()
        print(df[df['index']== 'sz000001'])
        time.sleep(3)
    pass

@task_time
def fetch_snapshot_task():
    df = fetch_realtime_price_all()
    print(df[df['index']== 'sz000001'][['index','name', 'now', 'time']])


#@sched.scheduled_job('cron',hour='9-15',minute='*', second="*/5", day_of_week='mon-fri')
@task_time
def save_to_hdf(df,file_name='/tmp/snapshot_t1.h5'):
    quotation = eq.use('sina')
    price_all = quotation.market_snapshot(prefix=True)
    df = pd.DataFrame(price_all).T.reset_index()
    print(df[df['index']=='sz000001'][['index','name','now','time']])
    df.to_hdf(file_name, key='df', mode='w')
    rq_util_log_info(f'save df to hdf at {file_name}, ok')


def read_from_hdf(file_name='/tmp/snapshot_t1.h5'):
    df = pd.read_hdf(file_name)
    rq_util_log_info(df)
    return df


if __name__ =='__main__':
    
    df = fetch_realtime_price_all()
    print(df)
    print(fetch_realtime_price_stock(['600519','300674','300146', '002236','605338']))
    #fetch_snapshot_timer()
    #fetch_snapshot_task()
    #save_to_hdf(df)
    #read_from_hdf()
    pass

