import os
import subprocess

USERPATH = os.path.expanduser('~')

subprocess.run(['killall','-9','streamlit'])

subprocess.run(args=["streamlit","run",f"{USERPATH}/rrshare/rrshare/rqWeb/stock_RS_OH_MA_to_streamlit.py"])



