#!/usr/bin/env bash

for t in $(find . -name "test*int*.py" | grep -v '.history'); do
  echo "Running $t"
  python -m pytest -s -v $t
done
