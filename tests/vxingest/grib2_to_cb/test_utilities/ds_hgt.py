"""
utility for comparing cfgrib data to wgrib2 data
"""

import xarray as xr

QUEUE_ELEMENT = "/opt/data/grib2_to_cb/input_files/2125214000000"
ds_height_above_ground_2m = xr.open_dataset(
    QUEUE_ELEMENT,
    engine="cfgrib",
    backend_kwargs={
        "filter_by_keys": {
            "typeOfLevel": "heightAboveGround",
            "stepType": "instant",
            "level": 2,
        },
        "read_keys": ["projString"],
    },
)
print("DewPoint\n")
ds_hgt_2_metre_dewpoint_temperature = ds_height_above_ground_2m.filter_by_attrs(
    long_name="2 metre dewpoint temperature"
)
jvals = [10, 20, 30, 40, 50, 150, 250, 350, 450, 550, 650, 750]
for j in jvals:
    print(
        str(j)
        + "\t"
        + str(
            ds_hgt_2_metre_dewpoint_temperature.variables[
                list(ds_hgt_2_metre_dewpoint_temperature.data_vars.keys())[0]
            ].values[j - 1, 10]
        )
        + "\n"
    )

ivals = [5, 25, 45, 65, 85, 105, 135, 155, 175, 195, 215, 235]
for i in ivals:
    print(
        str(i)
        + "\t"
        + str(
            ds_hgt_2_metre_dewpoint_temperature.variables[
                list(ds_hgt_2_metre_dewpoint_temperature.data_vars.keys())[0]
            ].values[10, i - 1]
        )
        + "\n"
    )

print("RH\n")
ds_hgt_2_metre_relative_humidity = ds_height_above_ground_2m.filter_by_attrs(
    long_name="2 metre relative humidity"
)
jvals = [10, 20, 30, 40, 50, 150, 250, 350, 450, 550, 650, 750]
for j in jvals:
    print(
        str(j)
        + "\t"
        + str(
            ds_hgt_2_metre_relative_humidity.variables[
                list(ds_hgt_2_metre_relative_humidity.data_vars.keys())[0]
            ].values[j - 1, 10]
        )
        + "\n"
    )

ivals = [5, 25, 45, 65, 85, 105, 135, 155, 175, 195, 215, 235]
for i in ivals:
    print(
        str(i)
        + "\t"
        + str(
            ds_hgt_2_metre_relative_humidity.variables[
                list(ds_hgt_2_metre_relative_humidity.data_vars.keys())[0]
            ].values[10, i - 1]
        )
        + "\n"
    )
