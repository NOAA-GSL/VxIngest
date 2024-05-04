import sys

import ncepbufr

# dump contents of bufr file to stdout or to a text file.
# Warning: resulting output may be HUGE.

if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} <bufrfile> <mnemonic>")
    sys.exit(1)
bufr = ncepbufr.open(sys.argv[1])
mnemonic = sys.argv[2]

bufr = ncepbufr.open(sys.argv[1])
first_dump = True  # after first write, append to existing file.
verbose = False  # this produces more readable output.
while bufr.advance() == 0:  # loop over messages.
    if mnemonic in bufr.msg_type:
        while bufr.load_subset() == 0:  # loop over subsets in message.
            # dump decoded data to a file.
            if first_dump:
                # clobber file if it exists
                bufr.dump_subset(sys.argv[2], verbose=verbose)
                first_dump = False
            else:
                # append to existing file.
                bufr.dump_subset(sys.argv[2], append=True, verbose=verbose)
bufr.close()
