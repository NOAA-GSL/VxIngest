# grib2 ingest to couchbase

## purpose

These programs are intended to import grib data into Couchbase taking advantage of the GSL Couchbase data schema
that has been developed by the GSL AVID model verification team.


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
.../VXingest/mats_metadata_and_indexes/ingest_models_from_grib.sh that is useful for importing
the associated metadata document.

## ingest documents

[obs ingest documents](https://github.com/NOAA-GSL/VxIngest/blob/0edaa03be13d75812e19ecf295e952b46d255b8f/mats_metadata_and_indexes/metadata_files/ingest_stations_and_obs_netcdf.json)

## Builder class

The builder is [NetcdfMetarObsBuilderV01](netcdf_builder.py)

There is a base NetcdfBuilder which has the generic code for reading a netcdf file and a specialized NetcdfMetarObsBuilderV01 class which knows how to build from a madis netcdf file.

## ingest documents - metadata

Refer to [ingest documents and metadata](https://github.com/NOAA-GSL/VxIngest/blob/77b73babf031a19ba9623a7fed60de3583c9475b/mats_metadata_and_indexes/metadata_files/README.md#L11)
