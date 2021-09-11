from apscheduler.schedulers.blocking import BlockingScheduler

from rrshare.rqUtil import (rq_util_date_today,
                            is_trade_time_secs_cn,
                            rq_util_if_tradetime, 
                            rq_util_if_trade, 
                            rq_util_datetime_to_strdate)

from rrshare.rqFactor.stock_RS_OH_MA import update_stock_PRS_new

# 创建调度器：BlockingScheduler
scheduler = BlockingScheduler()
today_ = rq_util_datetime_to_strdate(rq_util_date_today())

def job_update_stock_PRS_new():
    try:
        update_stock_PRS_new()
    except Exception as e:
        print(e)
        
scheduler.add_job(job_update_stock_PRS_new, 'interval', minutes=1, id='job_stock_PRS_new') # next_run_time=datetime.datetime.now(),

con = rq_util_if_trade(today_) and is_trade_time_secs_cn()  # tradedate and tradetime 9:25 start run calulate PRS
print('start con: ', con)

while con:
    scheduler.start()

