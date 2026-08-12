# Testing

These notes capture testing expectations for the RAOB pressure-level builder work.

The tests will be a combination of unit and integration tests. Unit tests should cover handlers and utility methods.
Integration tests should be patterned after existing GribBuilder integration tests.

The primary test data file will be the 00Z July 31, 2024 GRIB file.
For the two-thread integration test, use the 01Z July 31, 2024 file.

To make tests independent of NODD availability, these files should be downloaded and placed in [opt-data.gz](https://drive.google.com/file/d/1VWXoUEc0Lx5aXrtBfMK1yV5gF4iiG6H3/view?usp=drive_link).

## Unit tests

These focus on testing class methods and specific queries.

## Integration tests

These run the builder end-to-end with test data, then compare output files (without importing output) to expected outputs.
Expected output data may already exist in the database from prior validated manual imports.

## Test data

The required test data files are packaged in [opt-data.gz](https://drive.google.com/file/d/1VWXoUEc0Lx5aXrtBfMK1yV5gF4iiG6H3/view?usp=drive_link).

For this test suite, use July 31 00Z and July 31 01Z 2024 GRIB files.
[hrrr](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20140731/conus/hrrr.t00z.wrfprsfHH.grib2)
and
[hrrr](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20140731/conus/hrrr.t01z.wrfprsfHH.grib2)
for pressure level data files,
and
[hrrr](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20140731/conus/hrrr.t00z.wrfnatfHH.grib2)
and
[hrrr](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20140731/conus/hrrr.t01z.wrfnatfHH.grib2)
for native model step level data files.
