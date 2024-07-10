import sys

import ncepbufr

if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} <bufrfile> <mnemonic>")
    sys.exit(1)
bufr = ncepbufr.open(sys.argv[1])
mnemonic = sys.argv[2]

first_dump = True  # after first write, append to existing file.
verbose = False  # this produces more readable output.
while bufr.advance() == 0:  # loop over messages.
    if mnemonic == bufr.msg_type:
        vars = {
            "temperature":"TOB",
            "dewpoint":"TDO",
            "rh":"RHO",
            "specific_humidity":"QOB",
            "pressure":"POB",
            "height:":"ZOB",
            "wind_speed":"SOB",
            "U-Wind":"UOB",
            "V-Wind":"VOB",
            "wind_direction":"DDO",
        }
        while bufr.load_subset() == 0:  # loop over subsets in message.
            for _v in vars:
                val=bufr.read_subset(vars[_v]).squeeze()
                print(f"{_v}: {val}")
bufr.close()
