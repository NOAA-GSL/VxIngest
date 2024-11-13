# Testing

The tests will be a combination of unit and integration tests. The unit tests should cover the handlers and utility methods.
The integration tests will be patterned after the existing GribBuilder integration tests.
The test data file will be the 00Z July31, 20024 Grib File.
For the two thread integration test we will use 01Z July 31, 2024. In order to make the tests run independently of the NODD these files will be downloaded and placed in the [opt-data.gz](https://drive.google.com/file/d/1VWXoUEc0Lx5aXrtBfMK1yV5gF4iiG6H3/view?usp=drive_link) file.

## Unit tests

These will focus on testing class methods, and specific queries.

## Integration tests

These will run the builder with test data completely and then
compare output files (without importing any output) to expected outputs.
Expected output data might actually be in the database having been validated and imported manually.

## Test data

The necessary test data files will be found in the [opt-data.gz](https://drive.google.com/file/d/1VWXoUEc0Lx5aXrtBfMK1yV5gF4iiG6H3/view?usp=drive_link) file.

For this test suite the test will July 31 00Z July31, 20024 and July 31 01Z July31, 20024 Grib File(s).
[hrrr](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20140731/conus/hrrr.t00z.wrfprsfHH.grib2)
and
[hrrr](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20140731/conus/hrrr.t01z.wrfprsfHH.grib2)
for pressure level data files,
and
[hrrr](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20140731/conus/hrrr.t00z.wrfnatfHH.grib2)
and
[hrrr](https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20140731/conus/hrrr.t01z.wrfnatfHH.grib2)
for native model step level data files.
