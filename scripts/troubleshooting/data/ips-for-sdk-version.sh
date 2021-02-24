#!/usr/bin/env bash

# Output all IP addresses and the # of times connected for a given sdk

grep "libcouchbase/2.9.3-njs" memcached.log| \
  grep -oh -E "\[ [0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}" | \
  cut -b 3- | \
  sort | \
  uniq -c | \
  sort -nr
