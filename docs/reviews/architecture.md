# Architecture Overview

These notes capture architecture review context for the RAOB pressure-level builder work.

## Meeting

We held an online architecture review meeting on October 31. The general consensus was that the architecture is acceptable.

## Overview

The architecture plan is to extend the VxIngest GribBuilder and create a `GribModelRaobPressureBuilderV01` class to handle pressure model files. It is intended to read these files from NODD using the Boto3 Python package.

Example file: [hrrr.t00z.wrfprsf00.grib2](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20240731/conus/hrrr.t00z.wrfprsf00.grib2), which is an operational HRRR GRIB2 output file with pressure levels.

Example AWS CLI download command:

```bash
aws s3 cp --no-sign-request \
        s3://noaa-hrrr-bdp-pds/hrrr.20240731/conus/hrrr.t00z.wrfprsf00.grib2 \
        /opt/data/grib2_to_cb/hrrr_ops/input_files/2421300000000
```

This retrieves the 2024-07-31 00Z test file.

## Templates

There are associated ingest templates that define the data types, including:

- `MD:V01:RAOB:PRS:HRRR_OPS:ingest:grib2`

These are straightforward GRIB2 ingest templates. There will be a data document for each forecast hour and each level, with entries for every RAOB station. Drift information will be recorded in the data section.

## Data Source

The builder will use `cfgrib` to read temporary files, then clean them up. There does not appear to be a well-defined way to stream these files directly from AWS S3, so the program will download each file completely.

The primary isobaric dataset is retrieved with:

`ds = xr.open_dataset(f, engine="cfgrib", backend_kwargs={"filter_by_keys": {"typeOfLevel": "isobaricInhPa"}})`

This dataset contains variables such as temperature, height, dewpoint, and specific humidity.

Pressures in the GRIB2 file are spaced every 25 mb from 1013 mb through 50 mb, so the ingest must interpolate variables to standard levels (1010 through 20, spaced by 10).

## Method

Variables can be retrieved in Python by opening the file with xarray (using `cfgrib`), then accessing variable values for a given step and matching the pressure at that step.

```bash
# cd to the clone dir for VxIngest
> cd $HOME/VxIngest
# source the virtual env
> . .venv/bin/activate
# start python
> python
>>> # download the file see .... https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/s3/s3_basics/object_wrapper.py
>>> f="temp_grib2_file"
>>> ds=xr.open_dataset(f,engine="cfgrib",backend_kwargs={"filter_by_keys": {"typeOfLevel":"isobaricInhPa","read_keys": ["projString"]}})
# get the shape of the temperature variable
>>> ds.t.values.shape
(40, 1059, 1799)  # 40 levels, 1059 lat grid, 1799 lon grid (CONUS)
>>> list(ds.keys())
['gh', 't', 'r', 'dpt', 'q', 'w', 'u', 'v', 'absv', 'clwmr', 'unknown', 'rwmr', 'snmr', 'grle']

   # get the pressure values (this is a coordinate)
   >>> ds.coords['isobaricInhPa'].values
   array([1013., 1000.,  975.,  950.,  925.,  900.,  875.,  850.,  825.,
           800.,  775.,  750.,  725.,  700.,  675.,  650.,  625.,  600.,
           575.,  550.,  525.,  500.,  475.,  450.,  425.,  400.,  375.,
           350.,  325.,  300.,  275.,  250.,  225.,  200.,  175.,  150.,
           125.,  100.,   75.,   50.])
   # you find the pressure of interest and get its index..... for example 800mb is index 9, then use a gridpoint to the variable value
   >>>ds.t[9,100,100].values
   array(289.98505, dtype=float32). # this is in kelvin
   >>> ds.t[9,100,100].values * 9 / 5 - 459.67
1   np.float32(62.30307)  # this is in Fahrenheit
```

The builder will maintain a map of data variables that `translate_template_item` can use to access values. The example above does not include interpolation; the program will interpolate values to mandatory levels.
