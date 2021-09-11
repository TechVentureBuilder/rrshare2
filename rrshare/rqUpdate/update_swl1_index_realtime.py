from rrshare.rqSU import save_swl1_realtime
from rrshare.rqFactor import  swl1_index_realtime_prs


def record_swl1_index_realtime():
    save_swl1_realtime()
    swl1_index_realtime_prs()


if __name__  == '__main__':
    record_swl1_index_realtime()
