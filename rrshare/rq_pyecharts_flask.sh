#!/usr/bin/bash
set -o nounset
set -o errexit

export FLASK_APP=/home/rome/rrshare/rrshare/rqWeb/rq_pyecharts_flask.py
export FLASK_ENV=development

p=`lsof -i:5000 |wc -l`
if [ $p -eq 0 ];then
  /opt/anaconda3/bin/python /home/rome/rrshare/rrshare/rqWeb/rq_pyecharts_flask.py &
else
  echo "port 5000 is opened , flask is runing!"
fi


