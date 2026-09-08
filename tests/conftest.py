import os

# Ensure TESTING mode is enabled for the whole test suite so job criteria
# queries in main.py also match status='test' documents.
os.environ.setdefault("TESTING", "1")
