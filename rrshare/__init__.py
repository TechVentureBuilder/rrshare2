# -*- coding: utf-8 -*-

__version__ = '3.5.17'

#init setting
from rrshare.RQSetting.rqLocalize import (cache_path, log_path, rq_path, setting_path, make_dir_path)

# rqUtil code , date, tradedate
from rrshare.rqUtil import (rq_util_if_trade, rq_util_if_tradetime, is_trade_time_secs_cn, rq_util_get_last_tradedate)

# api
from rrshare.rqFetch import pro

# sql-util
from rrshare.rqUtil.rqPgsql import (PgsqlClass, client_pgsql, read_data_from_pg, read_table_from_pg,       
                                    read_sql_from_pg, save_data_to_postgresql)

#rqFactor
from rrshare.rqFactor.stock_RS_OH_MA import update_stock_PRS_day, update_stock_PRS_new

# record data
from rrshare.rqUpdate import (record_stock_day, record_stock_PRS, 
                    record_stock_PRS_new, record_swl_day, record_swl_rs_valuation)

# to streamlit
from rrshare.rqWeb import main_st, main_echart


#record data all
from rrshare.record_all_data import main_record

#cli
from rrshare.cmds import cli

def entry_point():
    cli()


__str__ = """\n
******* RRSHARE ******
"""

