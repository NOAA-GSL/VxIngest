#!/bin/sh
if [ $# -ne 1 ]; then
  echo "Usage $0 defaults-file"
  exit 1
fi
if [[ ! -f "$1" ]]; then
  echo "$1 is not a file - exiting"
  exit 1
fi
# find the minimum of the max's
mysql  --defaults-file="$1" -B -N -e "select max(time) from madis3.obs; select max(time) from ceiling2.obs; select max(time) from visibility.obs;" | sort -n | head -1
