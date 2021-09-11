# coding: utf-8
from rrshare.rqUtil import rq_util_get_pre_trade_date
from rrshare.rqSU import (  #rq_save_stock_day_pg,
                         rq_save_stock_list_pg,
                         rq_save_stock_day_adj_fillna_pg)

def record_stock_day(N=255):
    #print(rq_util_get_pre_trade_date(n=255))
    rq_save_stock_list_pg()
    #rq_save_stock_day_pg()
    rq_save_stock_day_adj_fillna_pg(rq_util_get_pre_trade_date(n=N))

if __name__ == '__main__':
    record_stock_day(255)

