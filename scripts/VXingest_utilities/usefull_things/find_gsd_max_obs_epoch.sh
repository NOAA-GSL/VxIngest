#!/bin/sh
if [ $# -ne 1 ]; then
  echo "Usage $0 credentials-file"
  exit 1
fi
if [[ ! -f "$1" ]]; then
  echo "$1 is not a file - exiting"
  exit 1
fi
credentials_file=$1
m_host=$(grep mysql_host ${credentials_file} | awk '{print $2}')
m_user=$(grep mysql_user ${credentials_file} | awk '{print $2}')
m_password=$(grep mysql_password ${credentials_file} | awk '{print $2}')
# find the minimum of the max's
mysql  -u${m_user} -p${m_password} -h${m_host}  -B -N -e "select max(time) from madis3.obs; select max(time) from ceiling2.obs; select max(time) from visibility.obs;" | sort -n | head -1
