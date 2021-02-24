#!/usr/bin/env bash

# set the cluster username / password
CB_USERNAME='Administrator'
CB_PASSWORD='password'

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  http://localhost:8091/pools/nodes | \
  jq -r 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
  .nodes[] | .hostname + " (" + (.services | join(", ")) + ")\n" +
  "  cpu_utilization_rate: " +
    ( .systemStats.cpu_utilization_rate | roundit/100.0 | tostring) + "%\n" +
  "  swap_total: " +
    ( .systemStats.swap_total / 1024 / 1024 | roundit/100.0 | tostring) + "MB\n" +
  "  swap_used: " +
    ( .systemStats.swap_used / 1024 / 1024 | roundit/100.0 | tostring) + "MB (" +
    ( (.systemStats.swap_used / .systemStats.swap_total) * 100 | roundit/100.0 | tostring) + "%)\n" +
  "  mem_total: " +
    ( .systemStats.mem_total / 1024 / 1024 | roundit/100.0 | tostring) + "MB\n" +
  "  mem_free: " +
    ( .systemStats.mem_free / 1024 / 1024 | roundit/100.0 | tostring) + "MB (" +
    ( (.systemStats.mem_free / .systemStats.mem_total) * 100 | roundit/100.0 | tostring) + "%)"
   '
