#!/usr/bin/env bash

# Output all connected SDK versions

grep -F "HELO" memcached.log | \
  grep -oh -E "((lib)?(couchbase)|gocbcore)[^ ]+" | \
  sort | \
  uniq -c | \
  sort -nr