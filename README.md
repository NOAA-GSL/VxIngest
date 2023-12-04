# VxIngest

VxIngest ingests meteorological data from various sources and makes it available in a document database for verification purposes in conjunction with the "Model Application Toolsuite" (MATS) application.

## Getting Started

VxIngest is a Python application, and uses [Poetry](https://python-poetry.org) for dependency management. [Ruff](https://docs.astral.sh/ruff/) is used in the codebase for linting & formatting. The repo follows a ["`src`"-style](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/) [layout](https://www.pyopensci.org/python-package-guide/package-structure-code/python-package-structure.html).

VxIngest outputs Couchbase JSON documents to disk as part of the "ingest" process. Those are then imported separately by a bash script in `scripts/VXingest_utilities/run-import.sh`.

## Usage

VxIngest is containerized for deployment. However, if you are developing the application, you can use Poetry to run locally. Note you will need a `config.yaml` file with connection information for a Couchbase instance.

`config.yaml`:

```yaml
cb_host: "url.for.couchbase"
cb_user: "user"
cb_password: "password"
cb_bucket: "vxdata"
cb_scope: "_default"
cb_collection: "METAR"
```

To run locally:
```bash
mkdir ~/output
poetry install
poetry run ingest -m ~/output/metrics -o ~/output/out -x ~/output/xfer -l ~/output/log -c config.yaml -j JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS
```

For debug output, you can set the `DEBUG` env variable:

```bash
mkdir ~/output
poetry install
env DEBUG=true poetry run ingest -m ~/output/metrics -o ~/output/out -x ~/output/xfer -l ~/output/log -c config.yaml -j JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS
```

## Development commands

Linting, formatting, type checking, and unit testing can be done through Poetry like so:

```bash
# Lint
poetry run ruff check .
# Format
poetry run ruff format .
# Type check
poetry run mypy src
# Unit test
CREDENTIALS=config.yaml poetry run pytest tests
```

## Container Build

You can build the docker container with the following:

```bash
$ docker build \
    --build-arg BUILDVER=dev \
    --build-arg COMMITBRANCH=$(git branch --show-current) \
    --build-arg COMMITSHA=$(git rev-parse HEAD) \
    --target=prod \
    -t vxingest/development/:dev \
    .
```

And run it like the below. Note the `data` and `public` env variables point to where the input data resides and where you'd like the container to write out to. These are currently (12/2023) mounted to `/opt/data` inside the container.

```bash
$ data=/data-ingest/data \
    public=/public \
    docker compose run ingest python -m ingest \
    -c /run/secrets/CREDENTIALS_FILE \
    -o /opt/data/test/outdir \
    -l /opt/data/test/logs \
    -m /opt/data/test/metrics \
    -x /opt/data/test/xfer"
```

## Docker Compose

There is currently a Docker Compose file with options to run unit tests and ingest from within the container. This may be a useful option for local development as well.

* `shell`: expects /data and /public for mounting
* `unit_test`: expects /opt/data for mounting
* `int_test`: expects /opt/data for mounting
* `ingest`: expects /data and /public for mounting
* `import`: expects /data for mounting

And can be run like:

```bash
data=/opt/data docker compose run unit_test
data=/opt/data docker compose run int_test
```

## Notes

See [`docs/general-notes.md`](docs/general-notes.md) for a general overview of Architecture, the data model and other useful things.

### Couchbase errata

See [`docs/couchbase.md`](docs/couchbase.md) for more on couchbase

## Disclaimer

This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an “as is” basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.
