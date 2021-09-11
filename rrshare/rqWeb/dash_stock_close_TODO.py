#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dash
import dash_core_components as dcc
import dash_html_components as html

from dash.dependencies import Input, Output

from rrshare.rqFetch import pro, jq
from rrshare.rqUtil import rq_util_get_last_tradedate, rq_util_log_info
from rrshare.rqFetch import stock_code_to_name


app = dash.Dash()

my_select_secs = ['600196.XSHG','300674.XSHE','300146.XSHE','002236.XSHE']
#codes = list(map(lambda x: x[0:6], my_select_secs))
options = []
for s in my_select_secs:
    sels = {}
    sels['label'] = stock_code_to_name(s[0:6])[0]
    sels['value'] = s
    options.append(sels)
rq_util_log_info(options)


app.layout = html.Div([
    html.H1('stock bar'),
    dcc.Dropdown(
    id = 'my_dropdown',
    options = options,
    value = my_select_secs[0]
    ),
    dcc.Graph(id='my-graph')
    ], style={'width': '500'})

@app.callback(Output('my-graph','figure'),[Input('my_dropdown','value')])
def update_graph(selected_dropdrown_value):

    #df = pro.daily(ts_code=selected_dropdrown_value, start_date='20210301', end_date=rq_util_get_last_tradedate().replace('-',''))
    df = jq.get_price(selected_dropdrown_value, count=250, end_date=rq_util_get_last_tradedate())
    #rq_util_log_info(f'\n {df}')

    return {
        'data':[
        {
            'x':df.index,
            'y':df.close, 
            #'y':df.low,
        }]}

app.css.append_css({'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})

def main():
    app.run_server(host='0.0.0.0',port=8503, debug=True)

if __name__ == '__main__':
    main()
    
    
  
