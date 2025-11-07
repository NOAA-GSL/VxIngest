import sys

import ncepbufr

# print inventory of specified bufr file

bufr = ncepbufr.open(sys.argv[1])
nsubsets = 0
inv = bufr.inventory()
for n, msg in enumerate(inv):
    out = (n + 1,) + msg
    print("message {}: {} {} {} {} subsets".format(*out))
    nsubsets += out[4]
bufr.close()
print(f"{nsubsets} total subsets in {len(inv)} messages")
