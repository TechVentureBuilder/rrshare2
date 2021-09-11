""" read data from pqsql;
    code --> name from table stock_list;
    swl_index --> name_level  from table swl_list;
    stock code belong to swl_level, L1,L2,L3 from table stock_belong_swl:
"""

import pandas as pd
from rrshare.rqUtil import read_data_from_pg, client_pgsql

def stock_code_to_name(code='',df=True):
    """ data get from local sql
        code --> 600519
        df=ture --> odataframe
        df=fasle --> dict 
    """
    stocks = read_data_from_pg(table_name='stock_list')[['code', 'name']]
    if code:
        if isinstance(code,str):
            code = [code]
        stock = stocks[stocks.code.isin(code)]
        if df:
            return stock
        return dict(zip(stock.code,stock.name))
    else:
        if df:
            return stocks
        return dict(zip(stocks.code,stocks.name))
        

def swl_index_to_name(index="", swl_level="", out_type=""):
    """index like 801020 not index_code 801020.SI
    swl_level= L1, L2, L3
        out_type =  dataframe, dict, list
    """
    swls = read_data_from_pg('swl_list')
    if swl_level:
        swls = swls[swls['level'] == swl_level][['index','name_level']]
    else:
        swls =swls[['index','name_level']]
    #print(swls)
    if index:
        if isinstance(index,list):
            swl = swls[swls['index'].isin(index)]
            swl_d = dict(zip(swl.index,swl.name_level))
            if out_type==dict:
                return swl_d
            if out_type==list:
                return list(swl_d.values())
            return swl
        else:
            return swls[swls['index'] == index]['name_level'].values[0]
    else:
        if out_type == dict:
            return dict(zip(swls.index,swls.name_level))
        return swls


def stock_code_belong_swl_name(code=None, swl_level=None):
    """ data get from local psql can select database--"rrshare"
        da change by client_pgsql("ttfactor") 
        swl_level --> L1 , L2, L3
        code --> 600519
        TODO
        #df=ture --> pd.dataframe
        #df=fasle --> dict 
    """
    stock_swls_all = read_data_from_pg(table_name='stock_belong_swl', client=client_pgsql("rrshare"))
    #print(stock_swls_all.head())
    #print(stock_swls_all.columns)
    if swl_level:
        swl_level_name = f"swl_{swl_level}"
        stock_swls = stock_swls_all[['code', 'name',swl_level_name]]
        #print(stock_swls)
    else:
        swl_level_name = ['swl_L1', 'swl_L2', 'swl_L3']
        cols =  ['code','name'] + swl_level_name
        #print(cols)
        stock_swls = stock_swls_all[cols]
        print(stock_swls.head())
    if code:
        if isinstance(code,str):
            code = [code]
        stock_swl = stock_swls[stock_swls.code.isin(code)]
        return stock_swl
    else:
        return stock_swls
       

if __name__ == '__main__':
    
    #print(stock_code_to_name('000831'))
    #print(stock_code_to_name(['000001','300674']))
    print(stock_code_to_name())
   
    #print(swl_index_to_name('850541'))
    print(swl_index_to_name(['850543', '850542']))
    print(swl_index_to_name(swl_level='L2'))
    
    print(stock_code_belong_swl_name(swl_level='L1'))
    #print(stock_code_belong_swl_name(code=["600519", "300146"]))
    #print(stock_code_belong_swl_name(code=['000001','600519'],swl_level='L3'))
    pass


