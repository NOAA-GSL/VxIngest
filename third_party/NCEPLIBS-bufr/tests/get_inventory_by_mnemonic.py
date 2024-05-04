import sys

import ncepbufr

# print inventory of specified bufr file messages by mnemonic
if len(sys.argv) < 3:
    print(f'Usage: {sys.argv[0]} <bufrfile> <mnemonic>')
    sys.exit(1)
bufr = ncepbufr.open(sys.argv[1])
mnemonic = sys.argv[2]
nsubsets = 0
inv = bufr.inventory()
for n,msg in enumerate(inv):
    if msg[1] != mnemonic:
        continue
    out = (n+1,)+msg
    print('message {}: {} {} {} {} subsets'.format(*out))
    nsubsets += out[4]
bufr.close()
print(f'{nsubsets} total subsets in {len(inv)} messages')
