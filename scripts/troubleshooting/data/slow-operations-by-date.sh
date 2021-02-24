#!/usr/bin/env bash

# Output a count of the slow operations by date + hour 
grep -i "slow operation" memcached.log | \
  grep "command\":\"GET\"" | \
  grep -oE "2018-12-\d{2}T\d{2}" | \
  uniq -c