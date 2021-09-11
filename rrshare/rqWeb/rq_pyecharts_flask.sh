#!/usr/bin/bash
set -o nounset
set -o errexit

export FLASK_APP=/home/rome/rrshare/rrshare/rqWeb/rq_pyecharts_flask.py
export FLASK_ENV=development
/opt/anaconda3/bin/python /home/rome/rrshare/rrshare/rqWeb/rq_pyecharts_flask.py

