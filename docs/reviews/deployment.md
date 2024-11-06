# Deployment

## Data_request

## Process_spec

## Ingest docs

## Compose.yaml

This might be the first builder to use the mechanisms being ironed out in the data bundle meetings.
The builders are all part of the same container already.
I envision that there will be a data_request made to DSG (with a data_request object)
and then I will develop an associated process_spec, and ingest templates. We will run this in the cloud using Ian's kubernetes deployment.
We will have a monitoring container (probably keda) that will monitor s3 events from the NODD and pick up on the S3 events
that are associated with the "s3://noaa-hrrr-bdp-pds/hrrr.20240731/conus/" bucket.
It will start ingest containers with the event being sent to the ingest main which will query the CB
for the associated process_spec and from that get the associated ingest template and process the file.
The ingest builder will need to download the file from the bucket to a temporary folder in the container,
process it, and then remove the temporary file.

