from pathlib import Path

import xarray as xr


def test_read_level_types_file():
    try:
        queue_element = "/opt/data/grib2_to_cb/hrrr_ops/input_files/2421300000000"
        with Path('/tmp/level_types').open('r') as f:
            lines = [line.strip() for line in f if line.strip()]
        level_types = set(lines)
        for level_type in level_types:
            ds = xr.open_dataset(
                queue_element,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {
                        "typeOfLevel": level_type,
                        "stepType": "instant",
                    },
                    "read_keys": ["projString"],
                    "indexpath": "",
                },
            )
            vars = ds.variables.keys()
            print (f"\nlevel_type {level_type}")
            for var in vars:
                print (f"var: {var} {ds[var].long_name}")
    except Exception as e:
        AssertionError(f"Error reading level types file: {e}")

