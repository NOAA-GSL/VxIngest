# netcdf ingest to couchbase

## purpose

These programs are intended to import netcdf data into Couchbase taking advantage of the GSL Couchbase data schema
that has been developed by the GSL AVID model verification team.

## Approach

These programs use a load_spec YAML file to define which ingest templates are to be used, a credentials file to provide database authentication, command line parameters for run time options, and the associated ingest template documents from the database that are specified in the load_spec.yaml.

### load_spec example

This is the test/load_spec_netcdf_metar_obs.yaml file from this distribution.

``` yaml
load_spec:
  email: "randy.pierce@noaa.gov"
  ingest_document_ids: ['MD:V01:METAR:obs:ingest:netcdf']
```

The email is optional - currently not used.
The ingest_document_ids: ['MD:V01:METAR:obs:ingest:netcdf'] line defines
a list of metadata documents (might be just one). These documents define how the program will operate.
The 'MD:V01:METAR:obs:ingest:netcdf' value is the id of a couchbase metadata document.
This document MUST exist on the couchbase cluster defined by cb_host in an associated credentials file (the name of which is provided as a command line parameter) and MUST be readable by the cb_user.

## ingest documents

[obs ingest documents](https://github.com/NOAA-GSL/VxIngest/blob/0edaa03be13d75812e19ecf295e952b46d255b8f/mats_metadata_and_indexes/metadata_files/ingest_stations_and_obs_netcdf.json)

## Builder class

The builder is [NetcdfMetarObsBuilderV01](https://github.com/NOAA-GSL/VxIngest/blob/8758f5e12ed0b20166961c201721e0f5098c5474/netcdf_to_cb/netcdf_builder.py#L354)

There is a base NetcdfBuilder which has the generic code for reading a netcdf file and a specialized NetcdfMetarObsBuilderV01 class which knows how to build from a madis netcdf file.

## ingest documents - metadata

Refer to [ingest documents and metadata](https://github.com/NOAA-GSL/VxIngest/blob/77b73babf031a19ba9623a7fed60de3583c9475b/mats_metadata_and_indexes/metadata_files/README.md#L11)
