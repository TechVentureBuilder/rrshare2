from rrshare.rqFactor.stock_RS_OH_MA import update_stock_PRS_new
from rrshare.rqFactor.stock_RS_OH_MA import update_stock_PRS_day

def record_stock_PRS():
    try:
        update_stock_PRS_day()
    except Exception as e:
        print(e)

def record_stock_PRS_new():
    try:
        update_stock_PRS_new()
    except Exception as e:
        print(e)

if __name__ == '__main__':
    record_stock_PRS()
    record_stock_PRS_new()

