# coding : utf-8
import time
import datetime
from rrshare.rqUtil.rqParameter import TRADE_TIMES


def is_trade_time_secs_cn(time_: str=time.ctime()) -> bool:
    #print(time_)
    #print(datetime.datetime.now().strftime('%H:%M:%S'))
    time_hour_min =  time_.split(" ")[3]
    #print(time_hour_min)
    #print(TRADE_TIMES().TRADE_TIME_AM[0])
    con1 = (time_hour_min >= TRADE_TIMES().TRADE_TIME_AM_pre[0]) and (time_hour_min <= TRADE_TIMES().TRADE_TIME_AM_pre[1])
    #print(con1)
    con2 = (time_hour_min >= TRADE_TIMES().TRADE_TIME_PM_pre[0]) and (time_hour_min <= TRADE_TIMES().TRADE_TIME_PM_pre[1])
    #print(con2)
    con = con1 or con2
    return  con

def is_before_tradetime_secs_cn(time_: str=time.ctime()) -> bool:
    #print(time_)
    time_hour_min =  time_.split(" ")[3]
    #print(time_hour_min)
    return (time_hour_min < TRADE_TIMES().TRADE_TIME_AM_pre[0])

#print(is_trade_time_secs_cn())

#print(is_before_tradetime_secs_cn())




    
