# Test instructions

Use the repository's supported Python version from `.python-version` and install dependencies with uv.

```bash
uv sync --locked --dev
```

Many tests require a credentials file that points at a Couchbase instance.

## Running tests

Use uv to run your tests.

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

To skip tests that require external resources such as database connections and raw data files:

```shell
CREDENTIALS=config.yaml uv run pytest -m "not integration" tests
```

This currently skips most of the suite, but it is the fastest validation path for routine changes.

## Test data

Some integration tests require external data unpacked to `/opt/data`.

Each test directory also has a `testdata` directory that contains other test data that's checked into the repo. Ideally, we could add our test data here before we switch to generating it.

## tests

There are two kinds of tests in each test directory:

- integration - tests are named like grib2_to_cb/test/test_int_metar_model_grib.py
- unit - tests are named like grib2_to_cb/test/test_unit_metar_model_grib.py

Notice the `test_int_` and `test_unit_` prefixes in the names. Unit tests are relatively independent, require minimal external test data, and run quickly. Integration tests require external data and configuration, are not independent, and may be long-running.

## vscode

To run tests from VS Code, create a `.env` file with the `CREDENTIALS` variable set so the editor picks it up.

```yaml
CREDENTIALS=config.yaml
```
