# Architecture overview

## Meeting:

We had an online review meeting on October 31.

## Overview:

The architecture extends the VxIngest GribBuilder to create a GribModelRaobPressureBuilderV01 class that will handle the pressure model files. It is intended to read these files from the NODD using the BOTO3 python package. Example https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20240731/conus/hrrr.t00z.wrfprsf00.grib2. is the operational hrrr model grib2 output file with pressure levels. using aws cli it would be "aws s3 cp --no-sign-request s3://noaa-hrrr-bdp-pds/hrrr.20240731/conus/hrrr.t00z.wrfprsf00.grib2 /opt/data/grib2_to_cb/hrrr_ops/input_files/2421300000000" to download the test data file of July31, 2024 00Z.

## Templates:

There are associated ingest templates that will define the data types.

## Data Source:

The builder will use cfgrib to read the temporary files, then clean them up after. The primary isobaric dataset is retrieved by ds=xr.open_dataset(f,engine="cfgrib",backend_kwargs={"filter_by_keys": {"typeOfLevel":"isobaricInhPa"}}) which will contain the variables we need, i.e. temp, height, dp, sh, etc. The pressures in the grib2 file are spaced every 25 mb from 1013mb through 50mb so the ingest will need to interpolate the variables to standard levels (1010 through 20 spaced by 10).

## Method:

Variables can retrieved in python by first opening the file with xarray (with the engine cfgrib), then accessing the variable values for a given step and matching the pressure at that step. i.e.

```bash
# cd to the clone dir for VxIngest
> cd $HOME/VxIngest
# source the virtual env
> . .venv/bin/activate
# start pyton3
> python
>>> # download the file see .... https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/s3/s3_basics/object_wrapper.py
>>> f="temp_grib2_file"
>>> ds=xr.open_dataset(f,engine="cfgrib",backend_kwargs={"filter_by_keys": {"typeOfLevel":"isobaricInhPa","read_keys": ["projString"]}})
# get the shape of the temperature variable
>>> ds.t.values.shape
(40, 1059, 1799). # 40 levels 1059 lat grid 1799 lon - this is conus
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
1   np.float32(62.30307).  # this is in fahrenheit
```

