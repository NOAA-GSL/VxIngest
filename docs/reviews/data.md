# Data Requirements

These notes capture data requirements for the RAOB pressure-level builder work.

## Data Source

The data will come from `s3://noaa-hrrr-bdp-pds/hrrr.20240731/conus/` in files such as `hrrr.t01z.wrfprsf04.grib2`.

The `wrfprs` token indicates a pressure file. A native step file uses `wrfnat`.

The ingest should support both:

- ongoing operational processing (process each file as it arrives)
- on-demand processing over a requested date range

The associated data request document is `DR:continuous:HRRR_OPS:1730496755:0:1730498583:V01`.

Since this data comes from the public [NODD](https://www.noaa.gov/information-technology/open-data-dissemination),
it does not need to be moved to a GSL S3 bucket and can be read directly from the source.

The file path includes a date component. In this example, `20240731` represents July 31, 2024. The file name `hrrr.t01z.wrfprsf04.grib2` contains:

- cycle time `t01z` (operational HRRR runs hourly)
- forecast hour `f04` (operational HRRR here is recorded through forecast hour 15)

## Data Output

The builder will produce a data bundle per run that includes required variables for all unprocessed model data where newer GRIB2 files are available than the latest model data currently in the database.

The builder is event-triggered by new model file creation, but should also process older unprocessed data within configured limits. This can be done by querying load job documents for the most recently processed document.

There should also be a way to provide parameters specifying an epoch range to process even when that data is older than the latest data in the database. The trigger mechanism for that flow is likely manual.

## Database Import

The data bundle will be imported according to the use cases (specifically UC 03-01) discussed in data bundle meetings.

## Data Bundle storage

Long-term data bundle storage is currently being discussed in data bundle meetings (UC-02-01). The intent is to use AWS storage classes to manage bundle lifecycle.

## Data expiration

This data will have a long TTL (time to live). How to specify TTL for long-lived operational data is still under discussion. The expectation is that Couchbase TTL will be specified in the `processSpec`, but this is not final.
