import requests
from bs4 import BeautifulSoup
import pandas as pd

EastmoneyHeaders = {
        'Host': 'data.eastmoney.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; Touch; rv:11.0) like Gecko',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Referer': 'http://data.eastmoney.com/bbsj/202112.html',
    }

def gen_secid(rawcode: str) -> str:
    '''
    生成东方财富专用的secid

    Parameters
    ----------
    rawcode : 6 位股票代码

    Return
    ------
    str: 指定格式的字符串

    '''
    # 沪市指数
    if rawcode[:3] == '000':
        return f'1.{rawcode}'
    # 深证指数
    if rawcode[:3] == '399':
        return f'0.{rawcode}'
    # 沪市股票
    if rawcode[0] != '6':
        return f'0.{rawcode}'
    # 深市股票
    return f'1.{rawcode}'

def fetch_stock_forecast_em(report_date="2021-06-30", **kwage):
    ''' report date
        df  
    '''
    day = '27'
    for p in range(1, 2):
        url = f"http://datacenter-web.eastmoney.com/securities/api/data/v1/get?\
            callback=jQuery112308107781430773868_1622071079060&\
                sortColumns=NOTICE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=5&\
                    pageNumber={p}&reportName=RPT_PUBLIC_OP_NEWPREDICT&columns=ALL&\
                        token=894050c76af8597a853f5b408b759f5d&filter=(REPORT_DATE%3D%{day}{report_date}%{day})"

        resp = requests.get(url)#, headers=EastmoneyHeaders)
        #print(resp)
        #print(resp.text)
        df = pd.DataFrame()
        #for i in range(1,10):
        resp_json = resp.json()['result']['data']
        print(resp_json)
        #df = df.append(pd.DataFrame.from_dict(resp_json, orient='index'))
        #print(df.T)
        #return(df.T)

if __name__ == "__main__":
    fetch_stock_forecast_em()