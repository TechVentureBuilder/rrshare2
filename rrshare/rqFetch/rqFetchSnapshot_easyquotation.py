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
            rq_util_log_info(time.time() - start_time)
            time.sleep(5)
        return data
    return in_func
    
    
def fetch_realtime_price_all(df=True,src='sina'): # or ['tecent','qq']
    quotation = eq.use(src)
    price_all = quotation.market_snapshot(prefix=True)
    if not df:
        return price_all
    price_df = pd.DataFrame(price_all).T.reset_index()
    return price_df

def fetch_etf_easyq(index_id="", min_volume=0, max_discount=None, min_discount=None):
    # TODO
    quotation = eq.use(['tecent', 'qq'])
    return quotation.etfindex()


def save_to_hdf(df,file_name='/tmp/snapshot_t1.h5'):
    while True:
        quotation = eq.use(src)
        price_all = quotation.market_snapshot(prefix=True)
        df = pd.DataFrame(price_all).T.reset_index()
        print(df[df['index']=='sz000001'][['index','name','now','time']])
        df.to_hdf(file_name, key='df', mode='w')
        rq_util_log_info(f'save df to hdf at {file_name}, ok')
        time.sleep(5)


def read_from_hdf(file_name='/tmp/snapshot_t1.h5'):
    df = pd.read_hdf(file_name)
    rq_util_log_info(df)
    return df


if __name__ =='__main__':
    
    df = fetch_realtime_price_all()
    print(df)
    #save_to_hdf(df)
    #read_from_hdf()
    pass

