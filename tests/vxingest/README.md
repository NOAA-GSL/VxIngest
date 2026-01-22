# Test instructions

You need a properly working python3 interpreter installed. we have seen issues where the python3 installation was a problem.

You need credential files in your home directory which can be retrieved (if you have permission) like this if you have cd'd into your home directory.

```scp www-data@model-vxtest.gsd.esrl.noaa.gov:~/adb-cb* .```

## Running tests

Use uv to run your tests. Make sure your virtual environment is setup with `uv sync --locked`

```shell
CREDENTIALS=config.yaml uv run pytest tests
```

You can specify certain directories to limit which tests are run.

```shell
CREDENTIALS=config.yaml uv run pytest tests/vxingest/ctc_to_cb
```

You can create a coverage report with:

```shell
CREDENTIALS=config.yaml uv run coverage run -m pytest tests
uv run coverage report
uv run coverage html
```

Then open `./htmlcov/index.html` in your browser for a detailed dive into what lines were run by the test suite.

Lastly, you can disable tests that require external resources (database connections & raw data files) like so:

```shell
CREDENTIALS=config.yaml uv run pytest -m "not integration" tests
```

Note that this currently (as of 1/2024) disables most of the tests.

## Test data

For now, you'll need test resources from: https://drive.google.com/drive/folders/18YY74S8w2S0knKQRN-QxZdnfRjKxDN69?usp=drive_link unpacked to `/opt/data` in order to run the test suite.

Each test directory also has a `testdata` directory that contains other test data that's checked into the repo. Ideally, we could add our test data here before we switch to generating it. 

## tests

There are two kinds of tests in each test directory.

- integration - tests are named like grib2_to_cb/test/test_int_metar_model_grib.py
- unit - tests are named like grib2_to_cb/test/test_unit_metar_model_grib.py

Notice the ***test_int_*** and the ***test_unit*** in the names.
Unit tests are relatively independent, require minimal external test data, and run quickly. These tests are for testing methods or functions independently. Integration tests require external data and configuration, are not independent, and may be very long running. These tests are for testing a working system, or components of a working system that are interacting.

## vscode

To run tests from VSCode, you'll need to create a `.env` file with the `CREDENTIALS` variable set so that VSCode picks up the `CREDENTIALS` env variable.

```yaml
CREDENTIALS=config.yaml
```
