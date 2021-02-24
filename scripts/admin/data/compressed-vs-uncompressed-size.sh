#!/usr/bin/env bash

# set the cluster username, password, bucket
CB_USERNAME='Administrator'
CB_PASSWORD='password'
BUCKET='demo'

/opt/couchbase/bin/cbstats \
  localhost:11210 all \
  -u $CB_USERNAME \
  -p $CB_PASSWORD \
  -b $BUCKET | \
  grep "itm_memory"