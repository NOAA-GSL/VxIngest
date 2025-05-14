# Deployment

This might be one of the first builders to use the mechanisms being ironed out in the data bundle meetings.

The actual builders are all part of the same container already, and will be available as soon as the builder PR is merged into the main branch.

We will run this in the cloud using Ian's kubernetes deployment.

## Data_request

There is an associated Data_Source object: "DS:continuous:RAOB:HRRR_OPS:1730496755:0:1730498583:V01"

## Process_spec

There is an associated Process_Spec:
"PS:RAOB:GRIB2:MODEL:HRRR_OPS:1730496755:1814400:V01"

## Ingest docs

There is an ingest doc for the pressure level ingest:
"MD:V01:RAOB:PRS:HRRR_OPS:ingest:grib2"

There is an ingest doc for the native level ingest:
"MD:V01:RAOB:NTV:HRRR_OPS:ingest:grib2"
