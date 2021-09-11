#!/bin/sh


st_id="`ps -a |grep streamlit`"
echo $st_id


case $1 in 
  start | begin | commence) 
    /home/rome/.local/bin/streamlit run /home/rome/rrshare/rrshare/rqWeb/stock_RS_OH_MA_to_streamlit.py &

  ;; 
  stop | end | destroy) 
    pkill -9 streamlit 
  ;; 
  restart | again) 
    killall -HUP streamlit 
  ;; 
  *) 
  echo "usage: streamlit [start | begin | commence | stop | end | destory | restart | again]" 
  ;; 
esac 


exit 0



