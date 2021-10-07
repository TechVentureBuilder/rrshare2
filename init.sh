#!/bin/bash

#1.copy rrshare-config.json to ~/.rrshare/setting/config.json
#2.install postgresql -- move data path to /mnt/disk1/pgsql_data-- create database of rrshare, rrfactor 
#3.update(record) day-data of history --record-all-data
#3.create rrshare-streamlit job-- setup on reboot

configfile=rrshare-config.json
#read -p "input rrshare setting config file(rrshare-config.jsion) : " configfile
echo $configfile
if  [ -f $configfile ];then
  cp $configfile /.rrshare/setting/config.json  && echo "copy rrshare-config.json to ~/.rrshare/setting/config.js  on"
fi

#mkdir -p ~/.rrshare/setting && 
#cp $conpfigfile ~/.rrshare/setting/config.json && echo "copy rrshare-config.json to ~/.rrshare/setting/config.json"

