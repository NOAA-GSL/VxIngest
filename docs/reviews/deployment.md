# Deployment

These notes capture deployment expectations for the RAOB pressure-level builder work.

This may be one of the first builders to use mechanisms being defined in the data bundle meetings.

The builders are all part of the same container and become available once the builder PR is merged to `main`.

The intended runtime is a cloud Kubernetes deployment.

## Data Request

There is an associated data source document:

`DS:continuous:RAOB:HRRR_OPS:1730496755:0:1730498583:V01`

## Process Spec

There is an associated process spec document:

`PS:RAOB:GRIB2:MODEL:HRRR_OPS:1730496755:1814400:V01`

## Ingest Documents

There is an ingest document for pressure-level ingest:

`MD:V01:RAOB:PRS:HRRR_OPS:ingest:grib2`

There is an ingest document for native-level ingest:

`MD:V01:RAOB:NTV:HRRR_OPS:ingest:grib2`
