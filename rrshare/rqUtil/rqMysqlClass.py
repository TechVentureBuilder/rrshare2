#!/usr/bin/python
# conding:utf-8

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

def conn_mysqldb(db='rrshare',host="127.0.0.1", user=user_mysql,passwd=password_mysql,port=3306):
    return pymysql.connect(host=host, user=user,passwd=passwd,database=db,port=port,charset='utf8')

def mysql_engine(db='rrshare',host="127.0.0.1",user=user_mysql,passwd=password_mysql,port=3306):
    try:
        return create_engine(f'mysql+pymysql://{user}:{passwd}.@{host}:{port}/{db}',encoding='utf8')
    except Exception as e:
        print(e)


class MysqlClass(object):
    
    def __init__(self, host="127.0.0.1", user=user_mysql, passwd=password_mysql):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = 3306
        self.charset = 'utf8mb4'
        self.conn_mysql =  pymysql.connect(host=self.host,user=self.user,passwd=self.passwd,port=self.port,charset=self.charset)

    def create_mysqlDB(self, db):
        try:
            cursor = self.conn_mysql.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db} ")
            print(f'create database {db} ok!')
        except Exception as e:
            print(e)

    def conn_mysqldb(self, db):
        return pymysql.connect(host=self.host,
                               user=self.user, 
                               passwd=self.passwd,
                               db = db,
                               charset=self.charset)
        
    def conn_engine(self, db):
        return create_engine(f'mysql+pymysql://{self.user}:{self.passwd}@{self.host}:{self.port}/{db}',encoding='utf8')


    def create_mysqlDB_table(self, sql, db, table):
        conn =  self.conn_mysqldb(db)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute(sql)
        print(f'create table "{table}" in datadase "{db}"')
        cursor.close()
        conn.close()


    def write_to_mysql(self,df,db,table,if_exists='append'):
        #与MySQL建立连接，
        conn = create_engine(f'mysql+pymysql://{self.user}:{self.passwd}@{self.host}:{self.port}/{db}',encoding='utf8')
        #写入数据，table为表名，‘replace’表示如果同名表存在就替换掉
        try:
            pd.io.sql.to_sql(df,f"{table}", conn, if_exists=if_exists)
            print(f'写入数据库{db}表{table}, ok')
        except Exception as e:
            print(e)


    def read_mysql_table(self, db, table,N):
        #与MySQL建立连接，
        conn = create_engine(f'mysql+pymysql://{self.user}:{self.passwd}@{self.host}:{self.port}/{db}',encoding='utf8')
        startTD = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(),N)
        startTD = f'"{startTD}"'   ### very important !!!
        #print(startTD)
        try:
            print(f'try to read data from {db}.{table} ...... ')
            sql = f"SELECT * FROM {table} WHERE time >= {startTD}"
            df = pd.read_sql(sql,conn)
            logging.info(f'read from database:{db} table:{table} N_tradedate \n {df.head()}')
            return df
        except Exception as e:
            print(e)

    def read__mysql_select(self,secs, db, table,N):
        #与MySQL建立连接，
        conn = create_engine(f'mysql+pymysql://{self.user}:{self.passwd}.@{self.host}:{self.port}/{db}',encoding='utf8')
        startTD = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(),N)
        startTD = f'"{startTD}"'   ### very important !!!
        secs = ','.join(f"'{x}'" for x in secs) 
        #print(secs)
        try:
            print(f'try to read data from {db}.{table} ...... ')
            sql = f"SELECT * FROM {table} WHERE time >= {startTD} AND code IN({secs})"
            df = pd.read_sql(sql,conn)
            logging.info(f'read from database:{db} table:{table} N_tradedate \n {df.head()}')
            return df
        except Exception as e:
            print(e)


if __name__ == '__main__':

    mysqldb = MysqlClass()
    #mysqldb.create_mysqlDB('test_rrshare')
    mysqldb.read_mysql_table(db='test_rrshare', table='stock_day', N=2)
    #write_to_mysql(df, 'test_rrshare','test','append')
    #print(read_mysql_table('gm','stock_day',19))
    pass

    
    
    
