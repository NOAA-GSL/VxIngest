# Data Requirements

## Data Source
The data will come from s3://noaa-hrrr-bdp-pds/hrrr.20240731/conus/ in form of hrrr.t01z.wrfprsf04.grib2 files as an example.
The 'wrfprs' part tells us it is a pressure file.
The ingest will need to process these on an ongoing 'operational' means, i.e. process every file that arrives there,
but also on an on-demand way where it is given a date range of required data.
This should be handled (I think) in the data_request, but that is still being discussed.

