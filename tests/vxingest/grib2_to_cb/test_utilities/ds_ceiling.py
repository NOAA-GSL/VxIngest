"""
utility for comparing cfgrib data to wgrib2 data
"""

import xarray as xr

QUEUE_ELEMENT = "/opt/data/grib2_to_cb/input_files/2125214000000"
ds_cloud_ceiling = xr.open_dataset(
    QUEUE_ELEMENT,
    engine="cfgrib",
    backend_kwargs={
        "filter_by_keys": {
            "typeOfLevel": "cloudCeiling",
            "stepType": "instant",
        }
    },
)
print("Ceiling\n")
jvals = [10, 20, 30, 40, 50, 150, 250, 350, 450, 550, 650, 750]
for j in jvals:
    print(
        str(j)
        + "\t"
        + str(
            ds_cloud_ceiling.variables[
                list(ds_cloud_ceiling.data_vars.keys())[0]
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
            ds_cloud_ceiling.variables[
                list(ds_cloud_ceiling.data_vars.keys())[0]
            ].values[10, i - 1]
        )
        + "\n"
    )
