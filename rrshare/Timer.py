# coding: utf-8
import datetime
import time

from apscheduler.schedulers.blocking import BlockingScheduler

from rrshare.rqFactor import update_swl_rs_valuation
#from rrshare.rqFactor.rq_ROE_CF_SR_INC import update_roe_cf_sr_inc
from rrshare.rqFactor.stock_RS_OH_MA import (update_stock_PRS_day,
                                             update_stock_PRS_new)
from rrshare.rqSU import (rq_save_stock_day_adj_fillna_pg,
                          #rq_save_stock_day_pg, 
                          rq_save_stock_list_pg,
                          rq_save_swl_industry_list_stock_belong,
                          rq_save_swl_day_pg)
from rrshare.rqUtil import (rq_util_date_today,
                            is_trade_time_secs_cn,
                            rq_util_if_tradetime, 
                            rq_util_if_trade, 
                            rq_util_datetime_to_strdate)

from rrshare.rqSU import save_swl1_realtime

from rrshare.rqFactor import swl1_index_realtime_prs

today_ = rq_util_datetime_to_strdate(datetime.date.today())


# 创建调度器：BlockingScheduler
scheduler = BlockingScheduler()
#scheduler = BackgroundScheduler()


def job_start_first_all():  # timer before start update first
    
    #update_roe_cf_sr_inc()

    rq_save_swl_industry_list_stock_belong()
    rq_save_swl_day_pg()

    rq_save_stock_list_pg()
    rq_save_stock_day_pg()
    rq_save_stock_day_adj_fillna_pg()

    update_swl_rs_valuation()

    update_stock_PRS_day()
    #update_stock_PRS()


def job_save_stock_day():
    rq_save_stock_list_pg()
    rq_save_stock_day_pg('2018-01-01')
    rq_save_stock_day_adj_fillna_pg('2018-01-01')


def job_save_swl_day():
    rq_save_swl_list_pg()
    rq_save_stock_belong_swl_pg()
    rq_save_swl_day_pg()


def job_update_stock_PRS_pre():
    update_stock_PRS_day()


def job_update_stock_PRS_new():
    update_stock_PRS_new()

def job_update_swl1_realtime_prs():
    save_swl1_realtime()
    swl1_index_realtime_prs()

def job_update_ROE_CF_SR_INC():
    #update_roe_cf_sr_inc()
    pass


def dojob():
    # job_start_first_all()
    # stock day
    scheduler.add_job(job_save_stock_day, 'cron', day_of_week='mon-sun',next_run_time=datetime.datetime.now(),
                      hour=19, minute=15, id='job_save_stock_day')
    # swl day
    scheduler.add_job(job_save_swl_day, 'cron', day_of_week='mon-fri',next_run_time=datetime.datetime.now(),
                      hour=21, minute=15, id='job_save_swl_day')
    # add stock PRS_pre
    scheduler.add_job(job_update_stock_PRS_pre, 'cron', 
                      day_of_week='mon-fri', hour=20, minute=30, next_run_time=datetime.datetime.now(),
                       id='job_stock_PRS_pre')
    # realtime PRS_new # run now() first
    scheduler.add_job(job_update_stock_PRS_new, 'interval', minutes=1, next_run_time=datetime.datetime.now(),
                      start_date='2021-04-05 9:28:00', end_date='2021-12-31 23:01:00', id='job_stock_PRS_new')

    # realtime swl1_prs
    scheduler.add_job(job_update_swl1_realtime_prs, 'interval', minutes=1, next_run_time=datetime.datetime.now(),
                      start_date='2021-05-05 9:26:00', end_date='2021-12-31 23:01:00', id='job_swl1_prs_realtime')


    # update_ROE_CF_SR_INC
    # scheduler.add_job(job_update_ROE_CF_SR_INC, 'cron', day_of_week='mon-sun',next_run_time=datetime.datetime.now(),
    #                  hour=8, minute=10, id='job_update_ROE_CF_SR_INC')

    scheduler.start()
    pass

#job_start_first_all()


def main_timer():
    scheduler.add_job(job_update_stock_PRS_new, 'interval', minutes=1, next_run_time=datetime.datetime.now(), id='job_stock_PRS_new')
    scheduler.add_job(job_update_swl1_realtime_prs,'interval', minutes=1, next_run_time=datetime.datetime.now(), id='job_swl1_prs_realtime')

    while  rq_util_if_trade(today_) and rq_util_if_tradetime():
        scheduler.start()

if __name__ == '__main__':
    main_timer()
    





