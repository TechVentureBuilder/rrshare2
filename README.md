# rrshare

## About rrshare:  
china stock market quant analysis;  
show web by streamliy;  
data source from tusharepro and eastmoney.  

## Quick Start:  
###Install rrshare  
1.git clone git@github.com:romepeng/rrshare.git
2. cd rrshare  
3. pip install .  
4. mkdir -p ~/.rrshare/setting
5. cp ~/config.json  ~/.rrshare/setting/config.json
6. install sql :  postgresql
7. rrshare init 
8. rrshare update-data
9. rrshare start-streamlit
10. set start 9 at setup
  create a start-streamlit.service use systemctl unit.
11. use crontab create timer:
	day(stock & swl) data timing update ;
	realtime-snapt data update every 1min at market open;

##Usage:
open webbroser
IP:8501

