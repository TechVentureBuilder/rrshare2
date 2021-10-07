#!/bin/bash
pyp="rrshare"
echo $HOME
cd $HOME/$pyp

dirp=`pwd`
echo $pyp
echo $dirp

pathpkg=$(pip show $pyp | grep -e "Location")
echo $pathpkg

crontab -l >> crontab-file

cat crontab-file

