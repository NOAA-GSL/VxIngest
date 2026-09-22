import os

# Ensure TESTING mode is enabled for the whole test suite so job criteria
# queries in main.py also match status='test' documents.
os.environ.setdefault("TESTING", "1")
#ensure credentials are set
os.environ.setdefault("CREDENTIALS","/Users/randy.pierce/credentials")
os.environ.setdefault("CREDENTIALS_FILE","/Users/randy.pierce/credentials")
