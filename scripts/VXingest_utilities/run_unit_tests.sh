#!/usr/bin/env bash

for t in $(find . -name "test*unit*.py" | grep -v '.history'); do
  echo "Running $t"
  python -m pytest -s -v $t
done
