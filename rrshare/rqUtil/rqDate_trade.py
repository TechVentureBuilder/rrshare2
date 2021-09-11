import time
import datetime
from typing import List, Tuple, Union
import tushare as ts
import pandas as pd

from rrshare.rqUtil.config_setting import setting
from rrshare.rqUtil.rqParameter import FREQUENCE, MARKET_TYPE
    
    
def rq_util_format_date2str(cursor_date):
    """
    explanation:
        对输入日期进行格式化处理，返回格式为 "%Y-%m-%d" 格式字符串
        支持格式包括:
        1. str: "%Y%m%d" "%Y%m%d%H%M%S", "%Y%m%d %H:%M:%S",
                "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H%M%S"
        2. datetime.datetime
        3. pd.Timestamp
        4. int -> 自动在右边加 0 然后转换，譬如 '20190302093' --> "2019-03-02"
    params:
        * cursor_date->
            含义: 输入日期
            类型: str
            参数支持: []
    """
    if isinstance(cursor_date, datetime.datetime):
        cursor_date = str(cursor_date)[:10]
    elif isinstance(cursor_date, str):
        try:
            cursor_date = str(pd.Timestamp(cursor_date))[:10]
        except:
            raise ValueError('请输入正确的日期格式, 建议 "%Y-%m-%d"')
    elif isinstance(cursor_date, int):
        cursor_date = str(pd.Timestamp("{:<014d}".format(cursor_date)))[:10]
    else:
        raise ValueError('请输入正确的日期格式，建议 "%Y-%m-%d"')
    return cursor_date


def get_trade_date_sse_tspro():
    token =  setting['TSPRO_TOKEN']
    pro = ts.pro_api(token=token)
    df = pro.trade_cal(exchange='SSE', start_date='20050101', end_date='20251231') #'19900101'
    trade_date = df[df.is_open == 1]['cal_date'].values
    trade_date = list(map(lambda x: rq_util_format_date2str(x),  trade_date))
    return trade_date
    
trade_date_sse = get_trade_date_sse_tspro()


def rq_util_get_next_period(datetime, frequence="1min"):
    """
    得到给定频率的下一个周期起始时间
    :param datetime: 类型 datetime eg: 2018-11-11 13:01:01
    :param frequence: 类型 str eg: '30min'
    :return: datetime eg: 2018-11-11 13:31:00
    """
    freq = {
        FREQUENCE.YEAR: "Y",
        FREQUENCE.QUARTER: "Q",
        FREQUENCE.MONTH: "M",
        FREQUENCE.WEEK: "W",
        FREQUENCE.DAY: "D",
        FREQUENCE.SIXTY_MIN: "60T",
        FREQUENCE.THIRTY_MIN: "30T",
        FREQUENCE.FIFTEEN_MIN: "15T",
        FREQUENCE.FIVE_MIN: "5T",
        FREQUENCE.ONE_MIN: "T",
    }
    return (pd.Period(datetime, freq=freq[frequence]) + 1).to_timestamp()


def rq_util_get_next_trade_date(
    cursor_date: Union[str, pd.Timestamp, datetime.datetime] = None, n: int = 1
) -> str:
    """
    得到后 n 个交易日 (如果当前日期为交易日，则不包含当前日期)
    e.g. 2020/12/25 为交易日，其后一个交易日为 2020/12/28; 2020/12/26 为非交易日，其后一个交易日为 2020/12/27
    Args:
        cursor_date(Union[str, pd.Timestamp, datetime.datetime], optional): 输入日期，默认为 None，即当天
        n(int, optional): 回溯交易日数目，默认为 1
    Returns:
        根据输入日期得到下 n 个交易日 (不包含当前交易日)
    """
    if not cursor_date:
        cursor_date = datetime.date.today().strftime("%Y-%m-%d")
    else:
        cursor_date = pd.Timestamp(cursor_date).strftime("%Y-%m-%d")
    if cursor_date in trade_date_sse:
        # 如果指定日期为交易日
        return trade_date_sse[trade_date_sse.index(cursor_date) + n]
    real_trade_date = rq_util_get_real_date(cursor_date, towards=-1)
    return trade_date_sse[trade_date_sse.index(real_trade_date) + n]


def rq_util_get_pre_trade_date(
    cursor_date: Union[str, pd.Timestamp, datetime.datetime] = None, n: int = 1
) -> str:
    """
    得到前 n 个交易日 (如果当前日期为交易日，则不包含当前日期)
    e.g. 2020/12/25 为交易日，其前一个交易日为 2020/12/24; 2020/12/26 为非交易日，其前一个交易日为 2020/12/25
    Args:
        cursor_date(Union[str, pd.Timestamp, datetime.datetime], optional): 输入日期，默认为 None，即当天
        n(int, optional): 回溯交易日数目，默认为 1
    Returns:
        str: 查询到的交易日
    """

    if not cursor_date:
        cursor_date = datetime.date.today().strftime("%Y-%m-%d")
    else:
        cursor_date = pd.Timestamp(cursor_date).strftime("%Y-%m-%d")
    if cursor_date in trade_date_sse:
        return trade_date_sse[trade_date_sse.index(cursor_date) - n]
    real_trade_date = rq_util_get_real_date(cursor_date, towards=1)
    return trade_date_sse[trade_date_sse.index(real_trade_date) - n]


def rq_util_get_last_tradedate():
    _now =  datetime.datetime.now()
    _hour = _now.hour
    _date = _now.strftime('%Y-%m-%d')
    #print(_now, _hour, _date)
    if  _date in trade_date_sse:
        if _hour > 19:
            return trade_date_sse[trade_date_sse.index(_date)]
        else:
            return trade_date_sse[trade_date_sse.index(_date) - 1]
    while _date not in trade_date_sse:
        _date = pd.to_datetime(str(_date)) + pd.Timedelta(days=-1)
        _date = _date.strftime('%Y-%m-%d')
    #print(_date) 
    #print(trade_date_sse[trade_date_sse.index(_date)])
    return trade_date_sse[trade_date_sse.index(_date)]


def rq_util_if_trade(day):
    """
    得到前 n 个交易日 (不包含当前交易日)
    '日期是否交易'
    查询上面的 交易日 列表
    :param day: 类型 str eg: 2018-11-11
    :return: Boolean 类型
    """
    if day in trade_date_sse:
        return True
    else:
        return False


def rq_util_if_tradetime(
    _time=datetime.datetime.now(), market=MARKET_TYPE.STOCK_CN, code=None
):
    """
    explanation:
        时间是否交易
    params:
        * _time->
            含义: 指定时间
            类型: datetime
            参数支持: []
        * market->
            含义: 市场
            类型: int
            参数支持: [MARKET_TYPE.STOCK_CN]
        * code->
            含义: 代码
            类型: str
            参数支持: [None]
    """
    _time = datetime.datetime.strptime(str(_time)[0:19], "%Y-%m-%d %H:%M:%S")
    if market is MARKET_TYPE.STOCK_CN:
        if rq_util_if_trade(str(_time.date())[0:10]):
            if _time.hour in [10, 13, 14]:
                return True
            elif (
                _time.hour in [9] and _time.minute >= 15
            ):  # 修改成9:15 加入 9:15-9:30的盘前竞价时间
                return True
            elif _time.hour in [11] and _time.minute <= 30:
                return True
            else:
                return False
        else:
            return False
    elif market is MARKET_TYPE.FUTURE_CN:
        date_today = str(_time.date())
        date_yesterday = str((_time - datetime.timedelta(days=1)).date())

        is_today_open = rq_util_if_trade(date_today)
        is_yesterday_open = rq_util_if_trade(date_yesterday)

        # 考虑周六日的期货夜盘情况
        if is_today_open == False:  # 可能是周六或者周日
            if is_yesterday_open == False or (
                _time.hour > 2 or _time.hour == 2 and _time.minute > 30
            ):
                return False

        shortName = ""  # i , p
        for i in range(len(code)):
            ch = code[i]
            if ch.isdigit():  # ch >= 48 and ch <= 57:
                break
            shortName += code[i].upper()

        period = [[9, 0, 10, 15], [10, 30, 11, 30], [13, 30, 15, 0]]

        if shortName in ["IH", "IF", "IC"]:
            period = [[9, 30, 11, 30], [13, 0, 15, 0]]
        elif shortName in ["T", "TF"]:
            period = [[9, 15, 11, 30], [13, 0, 15, 15]]

        if 0 <= _time.weekday() <= 4:
            for i in range(len(period)):
                p = period[i]
                if (
                    _time.hour > p[0] or (_time.hour == p[0] and _time.minute >= p[1])
                ) and (
                    _time.hour < p[2] or (_time.hour == p[2] and _time.minute < p[3])
                ):
                    return True

        # 最新夜盘时间表_2019.03.29
        nperiod = [
            [["AU", "AG", "SC"], [21, 0, 2, 30]],
            [["CU", "AL", "ZN", "PB", "SN", "NI"], [21, 0, 1, 0]],
            [["RU", "RB", "HC", "BU", "FU", "SP"], [21, 0, 23, 0]],
            [
                [
                    "A",
                    "B",
                    "Y",
                    "M",
                    "JM",
                    "J",
                    "P",
                    "I",
                    "L",
                    "V",
                    "PP",
                    "EG",
                    "C",
                    "CS",
                ],
                [21, 0, 23, 0],
            ],
            [["SR", "CF", "RM", "MA", "TA", "ZC", "FG", "IO", "CY"], [21, 0, 23, 30]],
        ]

        for i in range(len(nperiod)):
            for j in range(len(nperiod[i][0])):
                if nperiod[i][0][j] == shortName:
                    p = nperiod[i][1]
                    condA = _time.hour > p[0] or (
                        _time.hour == p[0] and _time.minute >= p[1]
                    )
                    condB = _time.hour < p[2] or (
                        _time.hour == p[2] and _time.minute < p[3]
                    )
                    # in one day
                    if p[2] >= p[0]:
                        if (
                            (_time.weekday() >= 0 and _time.weekday() <= 4)
                            and condA
                            and condB
                        ):
                            return True
                    else:
                        if (
                            (_time.weekday() >= 0 and _time.weekday() <= 4) and condA
                        ) or (
                            (_time.weekday() >= 1 and _time.weekday() <= 5) and condB
                        ):
                            return True
                    return False
        return False


def rq_util_get_next_day(date, n=1):
    """
    explanation:
        得到下一个(n)交易日
    params:
        * date->
            含义: 日期
            类型: str
            参数支持: []
        * n->
            含义: 步长
            类型: int
            参数支持: [int]
    """
    date = str(date)[0:10]
    return rq_util_date_gap(date, n, "gt")


def rq_util_get_last_day(date, n=1):
    """
    explanation:
       得到上一个(n)交易日
    params:
        * date->
            含义: 日期
            类型: str
            参数支持: []
        * n->
            含义: 步长
            类型: int
            参数支持: [int]
    """
    date = str(date)[0:10]
    return rq_util_date_gap(date, n, "lt")


def rq_util_get_last_datetime(datetime, day=1):
    """
    explanation:
        获取几天前交易日的时间
    params:
        * datetime->
            含义: 指定时间
            类型: datetime
            参数支持: []
        * day->
            含义: 指定时间
            类型: int
            参数支持: []
    """

    date = str(datetime)[0:10]
    return "{} {}".format(rq_util_date_gap(date, day, "lt"), str(datetime)[11:])


def rq_util_get_next_datetime(datetime, day=1):
    date = str(datetime)[0:10]
    return "{} {}".format(rq_util_date_gap(date, day, "gt"), str(datetime)[11:])


def rq_util_get_real_date(date, trade_list=trade_date_sse, towards=-1):
    """
    explanation:
        获取真实的交易日期
    params:
        * date->
            含义: 日期
            类型: date
            参数支持: []
        * trade_list->
            含义: 交易列表
            类型: List
            参数支持: []
        * towards->
            含义: 方向， 1 -> 向前, -1 -> 向后
            类型: int
            参数支持: [1， -1]
    """
    date = str(date)[0:10]
    if towards == 1:
        if pd.Timestamp(date) >= pd.Timestamp(trade_list[-1]):
            return trade_list[-1]
        while date not in trade_list:
            date = str(
                datetime.datetime.strptime(str(date)[0:10], "%Y-%m-%d")
                + datetime.timedelta(days=1)
            )[0:10]
        else:
            return str(date)[0:10]
    elif towards == -1:
        if pd.Timestamp(date) <= pd.Timestamp(trade_list[0]):
            return trade_list[0]
        while date not in trade_list:
            date = str(
                datetime.datetime.strptime(str(date)[0:10], "%Y-%m-%d")
                - datetime.timedelta(days=1)
            )[0:10]
        else:
            return str(date)[0:10]


def rq_util_get_real_datelist(start, end):
    """
    explanation:
        取数据的真实区间，当start end中间没有交易日时返回None, None,
        同时返回的时候用 start,end=rq_util_get_real_datelist
    params:
        * start->
            含义: 开始日期
            类型: date
            参数支持: []
        * end->
            含义: 截至日期
            类型: date
            参数支持: []
    """
    real_start = rq_util_get_real_date(start, trade_date_sse, 1)
    real_end = rq_util_get_real_date(end, trade_date_sse, -1)
    if trade_date_sse.index(real_start) > trade_date_sse.index(real_end):
        return None, None
    else:
        return (real_start, real_end)


def rq_util_get_trade_range(start, end):
    """
    explanation:
       给出交易具体时间
    params:
        * start->
            含义: 开始日期
            类型: date
            参数支持: []
        * end->
            含义: 截至日期
            类型: date
            参数支持: []
    """
    start, end = rq_util_get_real_datelist(start, end)
    if start is not None:
        return trade_date_sse[
            trade_date_sse.index(start) : trade_date_sse.index(end) + 1 : 1
        ]
    else:
        return None


def rq_util_get_trade_gap(start, end):
    """
    explanation:
        返回start_day到end_day中间有多少个交易天 算首尾
    params:
        * start->
            含义: 开始日期
            类型: date
            参数支持: []
        * end->
            含义: 截至日期
            类型: date
            参数支持: []
    """
    start, end = rq_util_get_real_datelist(start, end)
    if start is not None:
        return trade_date_sse.index(end) + 1 - trade_date_sse.index(start)
    else:
        return 0


def rq_util_date_gap(date, gap, methods):
    """
    explanation:
        返回start_day到end_day中间有多少个交易天 算首尾
    params:
        * date->
            含义: 字符串起始日
            类型: str
            参数支持: []
        * gap->
            含义: 间隔多数个交易日
            类型: int
            参数支持: [int]
        * methods->
            含义: 方向
            类型: str
            参数支持: ["gt->大于", "gte->大于等于","小于->lt", "小于等于->lte", "等于->==="]
    """
    try:
        if methods in [">", "gt"]:
            return trade_date_sse[trade_date_sse.index(date) + gap]
        elif methods in [">=", "gte"]:
            return trade_date_sse[trade_date_sse.index(date) + gap - 1]
        elif methods in ["<", "lt"]:
            return trade_date_sse[trade_date_sse.index(date) - gap]
        elif methods in ["<=", "lte"]:
            return trade_date_sse[trade_date_sse.index(date) - gap + 1]
        elif methods in ["==", "=", "eq"]:
            return date

    except:
        return "wrong date"


def rq_util_get_trade_datetime(dt=datetime.datetime.now()):
    """
    explanation:
        获取交易的真实日期
    params:
        * dt->
            含义: 时间
            类型: datetime
            参数支持: []
    """

    # dt= datetime.datetime.now()

    if rq_util_if_trade(str(dt.date())) and dt.time() < datetime.time(15, 0, 0):
        return str(dt.date())
    else:
        return rq_util_get_real_date(str(dt.date()), trade_date_sse, 1)


def rq_util_get_order_datetime(dt):
    """
    explanation:
        获取委托的真实日期
    params:
        * dt->
            含义: 委托的时间
            类型: datetime
            参数支持: []
    """

    # dt= datetime.datetime.now()
    dt = datetime.datetime.strptime(str(dt)[0:19], "%Y-%m-%d %H:%M:%S")

    if rq_util_if_trade(str(dt.date())) and dt.time() < datetime.time(15, 0, 0):
        return str(dt)
    else:
        # print('before')
        # print(rq_util_date_gap(str(dt.date()),1,'lt'))
        return "{} {}".format(rq_util_date_gap(str(dt.date()), 1, "lt"), dt.time())


def rq_util_future_to_tradedatetime(real_datetime):
    """
    explanation:
        输入是真实交易时间,返回按期货交易所规定的时间* 适用于tb/文华/博弈的转换
    params:
        * real_datetime->
            含义: 真实交易时间
            类型: datetime
            参数支持: []
    """
    if len(str(real_datetime)) >= 19:
        dt = datetime.datetime.strptime(str(real_datetime)[0:19], "%Y-%m-%d %H:%M:%S")
        return (
            dt if dt.time() < datetime.time(21, 0) else rq_util_get_next_datetime(dt, 1)
        )
    elif len(str(real_datetime)) == 16:
        dt = datetime.datetime.strptime(str(real_datetime)[0:16], "%Y-%m-%d %H:%M")
        return (
            dt if dt.time() < datetime.time(21, 0) else rq_util_get_next_datetime(dt, 1)
        )


def rq_util_future_to_realdatetime(trade_datetime):
    """
    explanation:
       输入是交易所规定的时间,返回真实时间*适用于通达信的时间转换
    params:
        * trade_datetime->
            含义: 真实交易时间
            类型: datetime
            参数支持: []
    """
    if len(str(trade_datetime)) == 19:
        dt = datetime.datetime.strptime(str(trade_datetime)[0:19], "%Y-%m-%d %H:%M:%S")
        return (
            dt if dt.time() < datetime.time(21, 0) else rq_util_get_last_datetime(dt, 1)
        )
    elif len(str(trade_datetime)) == 16:
        dt = datetime.datetime.strptime(str(trade_datetime)[0:16], "%Y-%m-%d %H:%M")
        return (
            dt if dt.time() < datetime.time(21, 0) else rq_util_get_last_datetime(dt, 1)
        )

