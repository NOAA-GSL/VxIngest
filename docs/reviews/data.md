# Data Requirements

## Data Source

The data will come from s3://noaa-hrrr-bdp-pds/hrrr.20240731/conus/ in form of hrrr.t01z.wrfprsf04.grib2 files.
The 'wrfprs' part tells us it is a pressure file. A native step file has 'wrfnat' instead.
The ingest will need to process these on an ongoing 'operational' means, i.e. process every file that arrives there.
The ingest will also need an on-demand way where it is given a date range of required data.
The data_request document is "DR:continuous:HRRR_OPS:1730496755:0:1730498583:V01"

Since this data comes from the public [NODD](https://www.noaa.gov/information-technology/open-data-dissemination)
it does not need to be moved to a GSL s3 bucket, it will be read directly from the data source.

The file path "s3://noaa-hrrr-bdp-pds/hrrr.20240731/conus/" includes a date component. In this example the "20240731" represents July 31, 2024. The file name "hrrr.t01z.wrfprsf04.grib2" contains a cycle time "t01z" (the operational hrrr runs every hour) and a forecast hour "f04" (the operational hrrr is recorded here to forecast hour 15).

## Data Output

The builder will produce a data bundle each run that will include the required variables for all of the unprocessed model data for which there are grib2 files available that are newer than the latest model data currently in the database. The builder is event triggered by the creation of a new model file but unprocessed data files should also be processed within limits. This can be done by querying the load job documents for the latest document processed. There will also be a way to provide parameters that will specify a range of epochs to process even though that data is older than the latest data in the database. It is a little unclear how that processing will be triggered, probably manually.

## Database Import

The data bundle will be imported according to the use cases (specifically UC 03-01)
that are currently being discussed in the data bundle meetings.

## Data Bundle storage

Long term data bundle storage is currently being discussed in the data bundle meetings. (UC-02-01). The intnent is to use AWS storage classes to manage bundle lifecycle.

## Data expiration

This data will have a very long TTL (Time To Live). It is currently being discussed how to specify the TTL long lived operational data. I expect that for Couchbase data the TTL will be specified in the process_Spec but this is still under discussion.
