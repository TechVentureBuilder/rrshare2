from rrshare.rqUpdate.update_stock_day import record_stock_day
from rrshare.rqUpdate.update_swl_day import record_swl_day
from rrshare.rqUpdate.update_swl_rs_valuation import record_swl_rs_valuation

from rrshare.rqUpdate.update_swl1_index_realtime import record_swl1_index_realtime

from rrshare.rqUpdate.update_stock_RS_OH_MA import record_stock_PRS, record_stock_PRS_new


def main_record():
    record_stock_day()
    record_swl_day()

    record_stock_PRS()
    record_stock_PRS_new()

    record_swl_rs_valuation()
    record_swl1_index_realtime()


if __name__ == '__main__':
    main_record()
