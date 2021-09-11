
import json
import tushare as ts 

from rrshare.rqUtil.config_setting import setting

try:
    token = setting['TSPRO_TOKEN']
    #ts.set_token(token)
    pro = ts.pro_api(token)
    #print('tushare token set ok , can use pro as api!')
except Exception as e:
    print(e)
    


