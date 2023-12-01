# Verifying cfgrib results against wgrib2

In an effort to be confident that we are parsing the grib file correctly I used the following manual checks to compare results against wgrib2.

## scripts in grib2_to_cb/test/test_utilities

These little routines are captured in "test_utilities"

### notes

Reference for [wgrib2](https://www.cpc.ncep.noaa.gov/products/wesley/wgrib2/)
This is a useful cheatsheet for wgrib2 [cheat sheet](https://www.ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/tricks.wgrib2)

This is the xarray [reference](https://docs.xarray.dev/en/stable/)
and this is the [xarray/cfgrib github page](https://github.com/ecmwf/cfgrib)  Look for the section that has ```For example to open US National Weather Service complex GRIB2 files you can use:```

use cfgrib.open_datasets(QUEUE_ELEMENT) to discover what datasets are available in the grib file.

cfgrib and wgrib2 are not indexed quite the same. The wgrib2 utility uses the option -ij -

``` text
-ij              inv   X Y    value of field at grid(X,Y) X=1,..,nx Y=1,..,ny (WxText enabled)
```

whereas cfgrib in xarray is indexed as y,x - you can see this by doing something like

``` bash
..$ ds_surface_pressure.variables[list(ds_surface_pressure.data_vars.keys())[0]]
<xarray.Variable (y: 1059, x: 1799)>
```

Also the wgrib2 utility starts indexing at 1 for x and y and the cfgrib via xarray starts with 0.

The test scripts are not automated but you can run them and visually see the outputs for ccomparison.
You can do this...
python ds_surface.py | tr '\t' ' '
and
./ds_surface.sh | awk -F"\t.*val=" '{print $1, $2}'
to get similar putput for comparing.

### Surface Pressure

#### cfgrib

Only this surface script is described here but there are other scripts in the test_utilities.

``` python
import xarray as xr
QUEUE_ELEMENT = '/opt/data/grib2_to_cb/input_files/2125214000000'
# Open the surface dataset
ds_surface = xr.open_dataset(QUEUE_ELEMENT,  engine='cfgrib',backend_kwargs={'filter_by_keys': {'typeOfLevel':'surface','stepType':'instant'}})
# retireve the Surface pressure data slice
ds_surface_pressure = ds_surface.filter_by_attrs(long_name="Surface pressure")
# define a range to iterate over
jvals=[10,20,30,40,50,150,250,350,450,550,650,750]
# for each grid location retrive the value and print out some output for comparing
for j in jvals:
    print (str(j) + "\t" + str(ds_surface_pressure.variables[list(ds_surface_pressure.data_vars.keys())[0]].values[j-1,0]) + "\n")
```

#### bash

``` bash
...$ export QUEUE_ELEMENT="/opt/data/grib2_to_cb/input_files/2125214000000"
for j in 10 20 30 40 50 150 250 350 450 550 650 750 ; do echo -e -n "$j\t"; wgrib2 -d 607 -ij 1 $j ${QUEUE_ELEMENT};echo; done
```


#### compare

visually compare the results....
