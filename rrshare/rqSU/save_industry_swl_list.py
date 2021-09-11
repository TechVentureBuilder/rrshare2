import time
import pandas as pd

from rrshare.rqUtil import rq_util_get_last_tradedate

from rrshare.rqFetch import  pro
#from rrshare.rqUtil import setting
from rrshare.rqUtil import (rq_util_code_tosrccode,rq_util_code_tostr)
#from rrshare.rqFetch.rqCodeName import (swl_index_to_name, stock_code_to_name)
from rrshare.rqUtil import PgsqlClass
from rrshare.rqFetch import Swsindex

conn = PgsqlClass().client_pg('rrshare')

class SwlIndexStock(object):
    """ first you need update -- stock_list
    then  save _swl_list;
    -->> save_swl_contains_stock
    -->> save_stock_belong_swl_L
    -->> save_stock_belong_swl_all

    """

    def __init__(self):
        #self.level = level
        self.LEVEL = ['L1','L2','L3']
        #self.symbol_to_tscode = lambda x: "".join([x,'.SH'] if str(x) > "333333" else "".join([x,'.SZ'])) 
        pass
    
    def all_stock_list_code(self,trade_date=rq_util_get_last_tradedate(), code_to_symbol=False):
        trade_date = str(trade_date).replace('-','')
        df = pro.adj_factor(trade_date=trade_date)
        if code_to_symbol:
            return list(df['ts_code'].apply(lambda x: x[0:6]).values)
        return list(df['ts_code'])

    
    def fetch_swl_index(self,level=""):
        df_tspro = pro.index_classify(level=level,src='SW')
        df_tspro['index'] = df_tspro['index_code'].map(lambda x: x[:6])
        df_tspro['name_level'] = df_tspro['industry_name'] + "_" + df_tspro['level']
        return df_tspro


    def save_swl_list(self):
        df_swl = self.fetch_swl_index()
        PgsqlClass().insert_to_psql(df_swl, 'rrshare','swl_list',if_exists='replace')


    def swl_index_to_name_dict(self,level=""):
        df = self.fetch_swl_index(level)
        return dict(zip(df['index_code'].values,df['name_level'].values))


    def stock_belong_swl_one(self,symbol):
        swl_index_code = self.fetch_swl_index()['index_code'].values
        fun = lambda x: "".join([x,'.SH'] if str(x) > "333333" else "".join([x,'.SZ'])) 
        ts_code = fun(symbol)
        df = pro.index_member(ts_code=ts_code)
        df = df[df.index_code.isin(swl_index_code)]
        df['name_level'] = df['index_code'].apply(lambda x : self.swl_index_to_name_dict()[x])

        return {ts_code : df['name_level'].values.tolist()}

    def swl_contains_stock(self):
        swl_all = pd.DataFrame()
        for level in self.LEVEL:
            #print(level)
            for l in self.fetch_swl_index(level)['index_code'].values:
                data = pro.index_member(index_code=l, fields='index_code, index_name,con_code, con_name, in_date')
                #print(data)
                swl_all = swl_all.append(data)
                swl_all['name_level'] = swl_all['index_name'] + "_" + str(level)
            #print(data)
        return swl_all

    def save_swl_contains_stock(self):
        swl_all = pd.DataFrame()
        for level in self.LEVEL:
            swl_l = pd.DataFrame()
            #print(level)
            for l in self.fetch_swl_index(level)['index_code'].values:
                data = pro.index_member(index_code=l, fields='index_code, index_name,con_code, con_name, in_date')
                #print(data)
                swl_l = swl_l.append(data)
                swl_l['name_level'] = swl_l['index_name'] + "_" + str(level)
            swl_l['level'] = level
            swl_all = swl_all.append(swl_l)
        PgsqlClass().insert_to_psql(swl_all,'rrshare','swl_contains_stock',if_exists='replace')
           

    def read_swl_contains_stock(self):
        from rrshare.rqUtil import PgsqlClass
        conn = PgsqlClass().client_pg('rrshare')
        df = PgsqlClass().read_psql('rrshare','swl_contains_stock')
        print(df)
        return df   


    def save_stock_belong_swl_L(self):
        df = self.read_swl_contains_stock()
        df = df.sort_values(by=['con_code'])
        df = df[['level', 'con_code','name_level']] #,'index_code']]
       
        for i,j in enumerate(["L1","L2","L3"]):
            #print(i, j)
            df_l = df[df.level == j]
            df_l.sort_values(by='con_code', inplace=True)
            df_l.rename(columns={'con_code':'ts_code'},inplace=True)
            df_l.set_index('ts_code', inplace=True)
            df_l.drop(columns=['level'], inplace=True)
            df_l.rename(columns={'name_level':f'swl_{j}'},inplace=True)
            df_l.reset_index(inplace=True)
            PgsqlClass().insert_to_psql(df_l, 'rrshare',f'stock_belong_swl_{j}',if_exists='replace')
    
        
    def save_stock_belong_swl_all(self):
        df0 = PgsqlClass().read_psql('rrshare','stock_list')[['ts_code','name','code']]
        df1 = PgsqlClass().read_psql('rrshare','stock_belong_swl_L1')
        df2 = PgsqlClass().read_psql('rrshare','stock_belong_swl_L2')
        df3= PgsqlClass().read_psql('rrshare','stock_belong_swl_L3')
        df_swl = pd.merge(df0,df1, how='left', on='ts_code')  
        df_swl = df_swl.merge(df2, how='left', on='ts_code')
        df_swl = df_swl.merge(df3, how='left', on='ts_code')
        #print(df_swl)
        PgsqlClass().insert_to_psql(df_swl, 'rrshare','stock_belong_swl',if_exists='replace')


def save_stock_belong_swsindex():
    swi = Swsindex()
    df_sws = swi.get_swl1_class()
    PgsqlClass().insert_to_psql(df_sws, 'rrshare','stock_belong_swl1',if_exists='replace')




def rq_save_swl_industry_list_stock_belong():
    swl =  SwlIndexStock()
    swl.save_swl_list()
    swl.save_swl_contains_stock()
    swl.save_stock_belong_swl_L()
    swl.save_stock_belong_swl_all()
    save_stock_belong_swsindex()
    
    


if __name__ == '__main__':
    #li = SwlIndexStock()
    #print(li.fetch_swl_index())
    #li.save_swl_list()
    #print(li.swl_index_to_name_dict())
    #print(li.stock_belong_swl('600196'))
    #df = li.swl_contains_stock()
    #print(df)
    #li.save_swl_contains_stock()
    #li.read_swl_contains_stock()
    #li.stock_belong_swl_pro()
    #li.save_stock_belong_swl_all()

    rq_save_swl_industry_list_stock_belong()
    pass

    

