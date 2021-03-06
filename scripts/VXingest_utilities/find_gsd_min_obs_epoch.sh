#!/bin/sh
if [ $# -ne 1 ]; then
  echo "Usage $0 defaults-file"
  exit 1
fi
if [[ ! -f "$1" ]]; then
  echo "$1 is not a file - exiting"
  exit 1
fi
# find the max of the mins
mysql  --defaults-file="$1" -B -N -e "select min(time) from madis3.obs; select min(time) from ceiling2.obs; select min(time) from visibility.obs;" | sort -n | tail -1
