#!/usr/bin/env bash

# set the cluster username / password
CB_USERNAME='Administrator'
CB_PASSWORD='password'

curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  http://localhost:8091/pools/default/buckets | \
  jq -r 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
  .[] | "Bucket: " + .name + "\n" +
  "  Quota Used: " + (.basicStats.quotaPercentUsed | roundit/100.0 | tostring) + "%\n" +
  "  Ops / Sec: " + (.basicStats.opsPerSec | tostring) + "\n" +
  "  Disk Fetches: " + (.basicStats.diskFetches | tostring) + "\n" +
  "  Item Count: " + (.basicStats.itemCount | tostring) + "\n" +
  "  Disk Used: " + (.basicStats.diskUsed / 1024 / 1024 | roundit/100.0 | tostring) + "MB\n" +
  "  Data Used: " + (.basicStats.dataUsed / 1024 / 1024 | roundit/100.0 | tostring) + "MB\n" +
  "  Memory Used: " + (.basicStats.memUsed / 1024 / 1024 | roundit/100.0 | tostring) + "MB\n" +
  "\n"'