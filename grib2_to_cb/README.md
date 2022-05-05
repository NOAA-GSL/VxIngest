# grib2 ingest to couchbase

## purpose

These programs are intended to import grib data into Couchbase taking advantage of the GSL Couchbase data schema
that has been developed by the GSL AVID model verification team.

## Environment

These programs require python3, and couchbase sdk 3.0 minimum (see [couchbase sdk](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html) )

In the test directory [README](test/README.md) you will find instructions for setting up the environment and for running the tests.

## Approach

These programs use a load_spec YAML file to define which ingest templates are to be used, a credentials file to provide database authentication, command line parameters for run time options, and the associated ingest template documents from the database that are specified in the load_spec.yaml.

### load_spec example

This is the grib2_to_cb/test/load_spec_grib_metar_hrrr_ops_V01.yaml file from this distribution.

```json
email: "randy.pierce@noaa.gov"
  ingest_document_ids: ['MD:V01:METAR:HRRR_OPS:ingest:grib2']
```

The email is optional - currently not used.
The cb_connection block defines the connection values that will be used to authenticate a connection to the host.
The ingest_document_ids: ['MD:V01:METAR:HRRR_OPS:ingest:grib2'] line defines
one or a list of metadata documents. These documents define how the program will operate.
The 'MD:V01:METAR:HRRR_OPS:ingest:grib2' value is the id of a couchbase metadata document.
This document MUST exist on the couchbase cluster defined by cb_host
and MUST be readable by the cb_user. Copies of the metadata documents are checked into
.../VXingest/mats_metadata_and_indexes/ingest_models_from_grib.json and there is a script
.../VXingest/mats_metadata_and_indexes/ingest_models_from_grib.sh that is usefull for importing
the associated metadata document.

## ingest documents

[obs ingest documents](https://github.com/NOAA-GSL/VxIngest/blob/0edaa03be13d75812e19ecf295e952b46d255b8f/mats_metadata_and_indexes/metadata_files/ingest_stations_and_obs_netcdf.json)

## Builder class

The builder is [NetcdfMetarObsBuilderV01](https://github.com/NOAA-GSL/VxIngest/blob/8758f5e12ed0b20166961c201721e0f5098c5474/netcdf_to_cb/netcdf_builder.py#L354)

There is a base NetcdfBuilder which has the generic code for reading a netcdf file and a specialized NetcdfMetarObsBuilderV01 class which knows how to build from a madis netcdf file.

## Credentials files

This is an example credentials file, the user and password are fake.

``` yaml
  cb_host: adb-cb1.gsd.esrl.noaa.gov
  cb_user: a_gsd_user
  cb_password: A_gsd_user_password

```

## ingest documents - metadata

Refer to [ingest documents and metadata](https://github.com/NOAA-GSL/VxIngest/blob/77b73babf031a19ba9623a7fed60de3583c9475b/mats_metadata_and_indexes/metadata_files/README.md#L11)

## Tests

There are tests in the test directory. To run the test_build_load_job_doc test
for example cd to the VXingest directory and use this invocation.
This assumes that you have cloned this repo into your home directory.

``` sh
source ~/VXingest/test_venv/bin/activate
export PYTHONPATH=~/VxIngest
python3 -m pytest grib2_to_cb/test/test_unit_metar_model_grib.py::TestGribBuilderV01Unit::test_build_load_job_doc
```

## Examples of running the ingest programs

### run_cron.sh

The current ingest invocations are contained in the [run_cron.sh](https://github.com/NOAA-GSL/VxIngest/blob/main/scripts/VXingest_utilities/run-cron.sh)

``` sh
outdir="/data/grib2_to_cb/hrrr_ops/output/${pid}"
mkdir $outdir
python ${clonedir}/grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/hrrr/conus/wrfprs/grib2 -m %y%j%H%f -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

```

### ingest parameters

-s is the load_spec
-c credential file
-p input grib2 file directory (this directory is automaticallly populated and purged out of band)
-m is the input file mask
-o is the directory where the output file documents will be placed
-t is the number of threads that the process will use

Each ingest process writes files to an output directory and then the generated document files are imported with the
[import_docs.sh](../scripts/VXingest_utilities/import_docs.sh utility)

### import parameters

-c credential file
-p the document directory (where the ingest process put its output fioes)
-n number of import processes to use
-l the log directory (each import process will create a temporary directory and then copy its logs to the log dir when it is finished importing)
