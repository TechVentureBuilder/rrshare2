# -*- coding: utf-8 -*-
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
from rrshare.rqUtil.rqSingleton import  ConSqlDb
from rrshare.rqUtil.rqLogs import (rq_util_log_debug, rq_util_log_expection,
                                             rq_util_log_info)

from rrshare.rqUtil import (rq_util_get_trade_range, rq_util_get_last_tradedate, 
                        rq_util_get_pre_trade_date, setting)

logging.basicConfig(level=logging.INFO, format=' %(asctime)s- %(levelname)s-%(message)s')
password_pgsql = setting['PGSQL_PASSWORD']


class PgsqlClass(ConSqlDb):
         
    def __init__(self,
            database ='postgres',
            user='postgres', 
            password=password_pgsql,
            host='localhost',
            port='5432'):

        self.database = database
        self.user =  user
        self.password = password
        self.host = host
        self.port = port

        #self.engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        #print(self.engine)
        self.conn = psycopg2.connect(user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database = database
        )
        #print(self.conn)
    
    def client_pg(self, db_name):
        client = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{db_name}')
        #print(client)
        return client


    def create_psqlDB(self,db_name):
        try:
            conn = self.conn
            #conn = psycopg2.connect(user=self.user,password=self.password,host=self.host, port=self.port)
            print(conn)
            psql = f'''CREATE DATABASE {db_name}'''
            #OWNER {self.user};
            #        GRANT ALL PRIVILEGES ON DATABASE {db_name}  to {self.user};
            #        '''
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(psql)
            #conn.commit()
            print(f'database {db_name} created successfully use by user  {self.user}')
            #cursor.close()
            conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print('Error while create PostgresSQL database', error)

   
    def create_table(self, db_name, table_name, tsql):
        try:
            conn = psycopg2.connect(user=self.user,password=self.password,host=self.host, port=self.port,database=db_name) 
            cursor = conn.cursor()
            conn.autocommit = True
            cursor.execute(f"DROP TABLE if EXISTS {table_name}")
            cursor.execute(tsql)
            #cursor.commit()
            print(f'{table_name} created  successfully on database {db_name}')
            conn.close()
        except Exception as e:
            print(e)


    def insert_to_psql(self, df,db_name, table_name, if_exists='append'):
        try:
            engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{db_name}')
            df.to_sql(name=table_name, con=engine, index=False, if_exists=if_exists)
            print(f'write to database:{db_name} table:{table_name} ok !')
        except Exception as e:
            print(e)

    def read_psql(self, db_name, table_name):
        engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{db_name}')
        return pd.read_sql_table(table_name,engine)
       
def client_pgsql(database='rrshare'):
    try:
        return PgsqlClass().client_pg(db_name=database)
    except Exception as e:
        print(e)     

def save_data_to_postgresql(data,table_name,client=client_pgsql(),if_exists='replace'):
        data.to_sql(table_name,client,index=False,if_exists=if_exists)
    
def load_data_from_postgresql(mes='',client=client_pgsql()):
    res=pd.read_sql(mes,client)
    return res

def read_data_from_pg(table_name='', client=client_pgsql()):
    ''' load all data of table_name
    '''
    res = pd.read_sql_table(table_name, client)
    return res


def read_table_from_pg(table_name='',period=1, client=None):
    '''only select trade_date, can't select columns
        default out lasttradedate data
        period -->int >= 1
    '''
    try: 
        start_date = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(), period-1)
        logging.info(start_date)
        sql = ''.join(['SELECT * FROM ', f'{table_name}', f" WHERE trade_date >= date_trunc('day', timestamp '"+start_date+"') order by trade_date ASC;"])
        logging.info(sql)
        res = pd.read_sql(sql,client)
        logging.info(f'\n {res}')
        return res
    except Exception as e:
        logging.info(e)


def read_sql_from_pg(start_date=None,data=None,table_name=None,client=client_pgsql()):
    '''can select columns and trade_date
    '''
    sql = ''.join(['SELECT ', data, ' FROM ', f'{table_name}', f" WHERE trade_date >= date_trunc('day', timestamp '"+start_date+"') order by trade_date ASC;"])
    #sql= 'select ' + data + ' from ' +table_name+" where trade_date >= date_trunc('day',timestamp '"+start_date+"') order by trade_date ASC;"
    try:
        t=time.time()
        res=pd.read_sql(sql,client)
        t1=time.time()
        tt = t1-t
        logging.info(f'read  data from {table_name} ,take {tt}')
        return res
    except Exception as e:
        logging.error(e)



def read_unique_data_from_pg(cols='*',table_name='',period=1, client=client_pgsql()):
    ''' select trade_date and columns( * is all), data GROUP BY trade_date and _ts_code --> duplicates 
    '''
    try: 
        start_date = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(), period-1)
        logging.info(start_date)
        sql = f"""
        SELECT {cols} 
        FROM  {table_name}
        GROUP BY trade_date, ts_code 
        """        
        logging.info(sql)
        res = pd.read_sql(sql,client)
        logging.info(f'\n {res}')
        return res
    except Exception as e:
        logging.info(e)



if __name__ == '__main__':
    import os
    psql = PgsqlClass()
    """
    psql.create_psqlDB(db_name='rrshare')
    path = '/home/rome/rrshare/rrshare/sql'
    file_n = 'stock_day_fillna.sql'
    file_path = os.path.join(path, file_n)
    print(file_path)
    with open(file_path, 'r') as f:
        tsql = f.read()
    #print(tsql)
    psql.create_table('rrshare','stock_day_fillna',tsql)
    """
    psql2 = PgsqlClass()
    print(id(psql), id(psql2))
    psql.client_pg(db_name='rrshare')
    
    print(read_data_from_pg('swl_list').head())
    print(read_table_from_pg(table_name='swl_day',client=client_pgsql('rrshare')))
    pass
