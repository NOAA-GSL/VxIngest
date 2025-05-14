import sys
from pathlib import Path

from tabulate import tabulate


def main():
    _wmoid = sys.argv[1] if sys.argv and len(sys.argv) > 1 else "65578"
    with Path(
        f"/opt/data/prepbufr_to_cb/test_artifacts/{_wmoid}_mandatory_values.txt"
    ).open() as _f:
        data = []
        qualified = False
        row = {}
        row_index = None
        while line := _f.readline():
            # skip empty lines - a qualified empty line means this subset is finished
            if not line.strip():
                if (
                    qualified
                    and row_index is None
                    and row
                    and row.get("press") != "null"
                ):
                    # if row doesn't exist in data it must be appended
                    data.append(row)
                row = {}
                qualified = False
                continue
            mnemonic = line.split()[0]
            if mnemonic == "POB":
                qualified = True
                _press = (
                    round(float(line.split()[1]))
                    if line.split()[1] != "MISSING"
                    else None
                )
                if _press not in [
                    1000,
                    850,
                    700,
                    600,
                    500,
                    400,
                    300,
                    250,
                    200,
                    150,
                    100,
                    70,
                    50,
                    30,
                    20,
                ]:
                    # not a standard level that we are interested in
                    continue
                # see if the row is already there (if this is a wind subset the row is already there).
                row_index = None
                for i, item in enumerate(data):
                    if "press" in item and item["press"] == _press:
                        row_index = i
                        break
                row = data[row_index] if row_index is not None else {"press": _press}
                continue
            if not qualified:
                continue
            else:  # still qualified
                try:
                    match mnemonic:
                        case "SID":
                            continue
                        case "TYP":
                            continue
                        case "PQM":
                            _val = extract_float_value(line)
                            if _val not in [0, 1, 2]:
                                # disqualified because of quality marker
                                # go to next POB
                                qualified = False
                                row = {}
                                continue
                        case "PPC":
                            _val = extract_float_value(line)
                            if _val != 1:
                                # disqualified because of program code
                                # go to next POB
                                qualified = False
                                row = {}
                                continue
                        case "QOB":
                            if qualified:
                                row["sh"] = line.split()[1]
                            continue
                        case "QQM":
                            _val = extract_float_value(line)
                            if _val not in [0, 1, 2, 9, 15]:
                                # disqualified because of quality marker
                                row["sh"] = None
                                continue
                        case "QPC":
                            _val = extract_float_value(line)
                            if _val != 1:
                                # disqualified because of program code
                                row["sh"] = None
                                continue
                        case "ZOB":
                            row["z"] = line.split()[1]
                            continue
                        case "ZQM":
                            _val = extract_float_value(line)
                            if _val not in [0, 1, 2]:
                                # disqualified because of quality marker
                                row["z"] = None
                                continue
                        case "ZPC":
                            _val = extract_float_value(line)
                            if _val != 1:
                                # disqualified because of program code
                                row["z"] = None
                                continue
                        case "TOB":
                            row["t"] = line.split()[1]
                            continue
                        case "TQM":
                            _val = extract_float_value(line)
                            if _val not in [0, 1, 2]:
                                # disqualified because of quality marker
                                row["t"] = None
                                continue
                        case "TPC":
                            _val = extract_float_value(line)
                            if _val != 1:
                                # disqualified because of program code
                                row["t"] = None
                                continue
                        case "TDO":
                            # does not need to be qualified
                            row["dp"] = line.split()[1]
                            continue
                        case "DDO":
                            row["wd"] = line.split()[1]
                            continue
                        case "FFO":
                            row["ws"] = line.split()[1]
                            continue
                        case "DFQ":
                            _val = extract_float_value(line)
                            if _val not in [0, 1, 2]:
                                # disqualified because of quality marker
                                row["wd"] = None
                                row["ws"] = None
                                continue
                        case "DFP":
                            _val = extract_float_value(line)
                            if _val != 1:
                                # disqualified because of program code
                                row["wd"] = None
                                row["ws"] = None
                                continue
                        case _:
                            print(f"Unknown mnemonic {mnemonic}")
                            continue
                except Exception as e:
                    print(f"Error: {e}")
    table = [
        [
            "press",
            "z",
            "t",
            "dp",
            "wd",
            "ws(knts)",
            "ws(FFO mps)",
        ]
    ]
    try:
        for row in data:
            if row.get("press") is None:
                continue
            table.append(
                [
                    row.get("press", "null"),
                    row.get("z", "null"),
                    row.get("t", "null"),
                    row.get("dp", "null"),
                    row.get("wd", "null"),
                    round(float(row.get("ws")) * 0.5144444444, 4)
                    if row.get("ws") is not None
                    else None,
                    row.get("ws", "null"),
                ]
            )
        print(tabulate(table, headers="firstrow", tablefmt="plain"))
    except Exception as e:
        print(f"Error: {e}")

def extract_float_value(line):
    _val = round(float(line.split()[1])) if line.split()[1] != "MISSING" else None
    return _val


if __name__ == "__main__":
    main()
