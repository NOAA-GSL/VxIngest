import sys

import ncepbufr

# dump contents of bufr file to stdout or to a text file by mnemonic.
# Warning: resulting output may be HUGE.

if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} <bufrfile> <mnemonic>")
    sys.exit(1)
bufr = ncepbufr.open(sys.argv[1])
mnemonic = sys.argv[2]

first_dump = True  # after first write, append to existing file.
verbose = True  # this produces more readable output.
while bufr.advance() == 0:  # loop over messages.
    if mnemonic == bufr.msg_type:
        while bufr.load_subset() == 0:  # loop over subsets in message.
            # dump decoded data to terminal.
            if len(sys.argv) > 3:
                # dump decoded data to a file.
                if first_dump:
                    # clobber file if it exists
                    bufr.dump_subset(sys.argv[3], verbose=verbose)
                    first_dump = False
                else:
                    # append to existing file.
                    bufr.dump_subset(sys.argv[3], append=True, verbose=verbose)
            else:
                bufr.print_subset(verbose=verbose)  # print decoded subset to stdout
bufr.close()
