#!/usr/bin/env bash

# Find active views

grep "GET" couchdb.log | \
  grep -E "/[A-Za-z0-9_]+/_design/[^? ]+" -oh | \
  sort | \
  uniq -c | \
  sort -nr