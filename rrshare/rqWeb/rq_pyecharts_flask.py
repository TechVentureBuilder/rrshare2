# flask_app.py
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from flask import Flask
from jinja2 import Markup, Environment, FileSystemLoader
from pyecharts.globals import CurrentConfig

# 关于 CurrentConfig，可参考 [基本使用-全局变量]
CurrentConfig.GLOBAL_ENV = Environment(loader=FileSystemLoader("/home/rome//rrshare/rrshare/templates"))
from rrshare.rqUtil import (client_pgsql,read_data_from_pg,read_sql_from_pg)
from rrshare.rqUtil import  (rq_util_get_pre_trade_date,rq_util_get_last_tradedate)

conn = client_pgsql('rrfactor')
lastTD = rq_util_get_last_tradedate()
    
cols_prs =  ['code','name',\
         'ma5','ma10','ma20', 'ma60', 'ma120','ma250',\
        'rs_5','rs_10','rs_20','rs_60','rs_120','rs_250',\
            'OH','OL','H','L','swl_L1','swl_L2','swl_L3']

from pyecharts import options as opts
from pyecharts.charts import Bar, Tab, Line, Pie
from pyecharts.components import Table

app = Flask(__name__, static_folder="templates")


def get_swl1_rs():
    data = pd.read_sql_table('swl_rs_L1',conn)
    df = data.copy()
    col = ['name','rs_5','rs_10','rs_20','rs_60','rs_120','rs_250']
    #col1 = col[1:]
    df = df[col]
    return df

def get_swl1_valuation():
    data = pd.read_sql_table('swl_rs_valuation_L1',conn)
    df = data.copy()
    col = ['name_x','pe','pb']
    #col1 = col[1:]
    df = df[col]
    return df

def get_etf_rs():
    data = pd.read_sql_table('etf_rs',conn)
    df = data.copy()
    col = ['name','turnover_rate','rs_3','rs_10','rs_60','rs_120']
    #col1 = col[1:]
    df = df[col]
    return df


def bar_RS() -> Bar:
    df = get_swl1_rs()
    print(df)
    col = df.columns
    print(col)
    x_axis = list(df.name.values)
    print(x_axis)
    y_5 =  list(df.rs_5.values)
    y_10 = list(df.rs_10.values)
    y_20 = list(df.rs_20.values)
    y_60 = list(df.rs_60.values)
    y_120 = list(df.rs_120.values)
    y_250 = list(df.rs_250.values)
    print(y_5,y_10,y_20, y_60,y_120, y_250)

    c = (
            Bar({"width": "1600px", "height":"660px"})
        .add_xaxis(x_axis)
        .add_yaxis("rs_5", y_5)
        .add_yaxis("rs_10", y_10)
        .add_yaxis("rs_20", y_20)
        .add_yaxis("rs_60", y_60)
        .add_yaxis("rs_120", y_120)
        .add_yaxis("rs_250", y_250)
        .set_global_opts(title_opts=opts.TitleOpts(title="swl1_rs", subtitle=f"{lastTD}" ) ,
            xaxis_opts=opts.AxisOpts(name_rotate=60, name="swl1_name", axislabel_opts={"rotate":45}))
    )
    return c


def bar_valucation() -> Bar:
    df = get_swl1_valuation()
    print(df)
    col = df.columns
    print(col)
    x_axis = list(df.name_x.values)
    print(x_axis)
    #y_turnover_ratio =  list(df.turnover_ratio.values)
    y_pe =  list(df.pe.values)
    y_pb =  list(df.pb.values)
    print(y_pe,y_pb)

    c = (
            Bar({"width": "1500px", "height":"560px"})
        .add_xaxis(x_axis)
        #.add_yaxis("turnover_ratio", y_turnover_ratio)
        .add_yaxis("pe", y_pe)
        .add_yaxis("pb", y_pb)

        .set_global_opts(title_opts=opts.TitleOpts(title="swl1-valuation", subtitle=f"{lastTD}" ),
             xaxis_opts=opts.AxisOpts(name_rotate=60, name="swl1_name", axislabel_opts={"rotate":45}) )
    )
    return c


def bar_etf_rs() -> Bar:
    df = get_etf_rs()
    print(df)
    col = df.columns
    print(col)
    x_axis = list(df.name.values)
    print(x_axis)
    y_1 = list(df.turnover_rate.values)
    y_3 =  list(df.rs_3.values)
    y_10 = list(df.rs_10.values)
    #y_20 = list(df.rs_20.values)
    y_60 = list(df.rs_60.values)
    y_120 = list(df.rs_120.values)
    print(y_3,y_10, y_60,y_120)

    c = (
            Bar({"width": "1380px", "height":"520px"})
        .add_xaxis(x_axis)
        .add_yaxis("turnover_rate", y_1)
        .add_yaxis("rs_3", y_3)
        .add_yaxis("rs_10", y_10)
       # .add_yaxis("rs_20", y_20)
        .add_yaxis("rs_60", y_60)
        .add_yaxis("rs_120", y_120)
        .set_global_opts(title_opts=opts.TitleOpts(title="etf_rs", subtitle=f"{lastTD}" ) ,
            xaxis_opts=opts.AxisOpts(name_rotate=60, name="etf_name", axislabel_opts={"rotate":45}))
    )
    return c


def table_base() -> Table:
    table = Table()
    df = pd.read_sql_table('stock_select_PRS', conn)
    headers = cols_prs
    df = df[headers]
    rows = df.values 
    table.add(headers, rows).set_global_opts(
        title_opts=opts.ComponentTitleOpts(title="")
    )
    return table


def table_RS_OH_MA() -> Table:
    table = Table()
    df = pd.read_sql_table('stock_RS_OH_MA',conn)
    headers =  cols_prs
    df = df[headers]
    rows = df.values 
    table.add(headers, rows).set_global_opts(
        title_opts=opts.ComponentTitleOpts(title="")
    )
    return table


def table_ROE_CF_SR_INC() -> Table:
    table = Table()
    df = pd.read_sql_table('ROE_CF_SR_INC',conn)
    headers = list(df.columns)
    df = df[headers]
    rows = df.values 
    table.add(headers, rows).set_global_opts(
        title_opts=opts.ComponentTitleOpts(title="")
    )
    return table


@app.route("/")
def index():
    tab = Tab()
    #tab.add(bar_etf_rs(), "etf_rs")
    tab.add(bar_RS(), "swl1-rs")
    tab.add(bar_valucation(), "swl1_valuation")
    tab.add(table_base(), "RS_select")
    tab.add(table_RS_OH_MA(),"RS_OH_MA")
    tab.add(table_ROE_CF_SR_INC(),"ROE_CF_SR_INC")
    return Markup(tab.render_embed())

def main():
    app.run(host='0.0.0.0', port=5000,debug=True)

if __name__ == "__main__":
    main()
    
    
