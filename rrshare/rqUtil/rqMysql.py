#!/usr/bin/python
# conding:utf-8
# TODO
import time
import logging
from typing import List, Tuple, Union
import numpy as np
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from rrshare.rqUtil.config_setting import setting
from rrshare.rqUtil.rqDate_trade import (rq_util_get_last_tradedate, rq_util_get_pre_trade_date)

logging.basicConfig(level=logging.DEBUG,format=' %(asctime)s-%(levelname)s-%(message)s ')

db_mysql ='rrshare'
user_mysql = 'root'
password_mysql = setting['MYSQL_PASSWORD']

logging.info(user_mysql)
logging.debug(password_mysql)


def conn_mysql(host="127.0.0.1", user=user_mysql,passwd=password_mysql,port=3306):
    return pymysql.connect(host=host, user=user,passwd=passwd,port=port,charset='utf8')
#logging.warning(conn_mysql())

def conn_mysqldb(db=db_mysql,host="127.0.0.1", user=user_mysql,passwd=password_mysql,port=3306):
    return pymysql.connect(host=host, user=user,passwd=passwd,database=db,port=port,charset='utf8')
#logging.info(conn_mysqldb('test_rrshare'))

def mysql_engine(db=db_mysql,host="127.0.0.1",user=user_mysql,passwd=password_mysql,port=3306):
    try:
        return create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}',encoding='utf8')
    except Exception as e:
        print(e)
#logging.warning(mysql_engine('test_rrshare'))


def mysql_conn(db=db_mysql):
    try:
        conn_sql = conn_mysql()
        cursor = conn_sql.cursor()
        #创建数据库，如果不存在
        sql = f"CREATE DATABASE IF NOT EXISTS {db}"
        #执行创建数据库
        cursor.execute(sql)
        logging.info(f'create mysql {db}')
    except AssertionError as error:
        print(error)
    else:
        try:
            conn = mysql_engine(db)
            logging.info(f'connect to mysql {db}')
            return conn
        except:
            logging.error(f'can not connect to mysql  {db}')
            

def mysql_create_table(sql, table_name, db=db_mysql):
    conn = conn_mysqldb(db) 
    try:
        cursor = conn.cursor()
        sql = sql.format(table_name)
        cursor.execute(sql)
        logging.info(f'create table {table_name} in datadase {db}')
    except Exception as e:
        print(e)


def write_to_mysql(df, db, table,if_exists='replace'):
    #与MySQL建立连接，
    conn = mysql_engine(db)
    #写入数据，table为表名，‘replace’表示如果同名表存在就替换掉
    try:
        pd.io.sql.to_sql(df,f"{table}", conn, if_exists=if_exists)
        print(f'写入数据库{db}表{table}, ok')
    except Exception as e:
        print(e)

def read_mysql_table(db,table,N):
    #与MySQL建立连接，
    conn = mysql_engine(db)
    #logging.info(conn)
    startTD = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(),N)
    startTD = f'"{startTD}"'   ### very important !!!
    logging.debug(startTD)
    #get table from db's table
    try:
        #logging.info(f'try to read data from {db}.{table} ...... ')
        sql = f"SELECT * FROM {table} WHERE bob >= {startTD}"
        df = pd.read_sql(sql,conn)
        logging.info(f'read from database:{db} table:{table} N_tradedate \n {df.head()}')
        return df
    except Exception as e:
        print(e)

def read_mysql_sql(data: Union[str,list,tuple] = None, 
                  table: Union[str] = None, 
                  db: str = db_mysql
                  )  -> pd.DataFrame:
    try:
        conn = mysql_engine(db)
        sql = ''.join(['SELECT ', data,  ' FROM ', f'{table}'])
        logging.info(sql)
        df = pd.read_sql(sql, conn)
        logging.info(f'read sql from {db}/{table}\n {df}')
        return df
    except Exception as e:
        logging.error(e)
#read_mysql_sql('select name,close from realtime_price', 'realtime_price','test_rrshare' )


def read_mysql_select(secs, db, table,N):
    #与MySQL建立连接，
    conn = mysql_engine(db)
    startTD = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(),N) 
    startTD = f'"{startTD}"'   ### very important !!!
    secs = ','.join(f"'{x}'" for x in secs) 
    logging.debug(secs) 
    try:
        print(f'try to read data from {db}.{table} ...... ')
        sql = f"SELECT * FROM {table} WHERE bob >= {startTD} AND symbol IN({secs})"
        df = pd.read_sql(sql,conn)
        logging.info(f'select date and code \n {df.tail()}')
        return df
    except Exception as e:
        print(e)


if __name__ == '__main__':
    """
    sql = '''CREATE TABLE stock_day(
        `trade_date` date NOT NULL,
        `code` varchar(45) NOT NULL,
        `open` decimal(20,2) DEFAULT NULL,
        `close` decimal(20,2) DEFAULT NULL,
        `high` decimal(20,2) DEFAULT NULL,
        `low` decimal(20,2) DEFAULT NULL,
        `volume` decimal(20) DEFAULT NULL,
        `amount` decimal(30,2) DEFAULT NULL,
        PRIMARY KEY (`trade_date`,`code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
    
    data = 'name , close'
    table = 'price' 
    sql = ''.join(['SELECT ', data,  ' FROM ', f'{table}'])
    logging.info(sql)
    """
    #mysql_conn('test_rrshare')
    #mysql_create_table(sql,'stock_day','test_rrshare')
    read_mysql_sql(data='name,pro_close,close,date,time',table='realtime_price',db='test_rrshare' )
    
    #logging.info(df)
    #write_to_mysql(df, 'test_rrshare','test')
    
    #read_mysql_table('gm','stock_day',3)
    #secs = ['SZSE.000001','SHSE.600519'] 
    #df = read_mysql_select(secs,'gm','stock_day',2)
    pass

    
