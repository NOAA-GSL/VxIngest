# Development Guide

This guide covers how to set up your environment to work on VxIngest. It also covers code standards, linting, and testing.

VxIngest is containerized for deployment. For more on using the application, see the [README.md](../README.md) in the repo root.

## Overview

VxIngest is a Python application, and uses [Poetry](https://python-poetry.org) for dependency management. [Ruff](https://docs.astral.sh/ruff/) is used in the codebase for linting & formatting. The repo follows a ["`src`"-style](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/) [layout](https://www.pyopensci.org/python-package-guide/package-structure-code/python-package-structure.html).

VxIngest outputs Couchbase JSON documents to disk as part of the "ingest" process. Those are then imported separately by a bash script in `scripts/VXingest_utilities/run-import.sh`.

## Getting Started

You will first need to download and install [Poetry](https://python-poetry.org/docs/#installation). We will use Poetry to manage our Python venv's and dependencies.

Poetry can be used to run the application locally. Note you will need a `config.yaml` or `credentials` file with connection information for a Couchbase instance.

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
mkdir tmp/output
poetry install
poetry run ingest \
    -m tmp/output/metrics \
    -o tmp/output/out \
    -x tmp/output/xfer \
    -l tmp/output/log \
    -c config.yaml \ # this is the path to the file with your database credentials
    -j JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS
```

For debug output, you can set the `DEBUG` env variable:

```bash
mkdir tmp/output
poetry install
env DEBUG=true poetry run ingest \
    -m tmp/output/metrics \
    -o tmp/output/out \
    -x tmp/output/xfer \
    -l tmp/output/log \
    -c config.yaml \ # this is the path to the file with your database credentials
    -j JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS
```

## Developer tools

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

### Testing

You will need some data files downloaded locally in order to use the test suite. For more details, see [tests/vxingest/README.md](../tests/vxingest/README.md).

If you are using VSCode, the test suite should be picked up automatically. However, to set the `CREDENTIALS` env variable in VSCode, you will want to put the value in a `.env` file in the root of the repo like so:

`.env`:

```env
CREDENTIALS=config.yaml
```

### Container Build

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

### Docker Compose

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

### General

See [`docs/general-notes.md`](docs/general-notes.md) for a general overview of Architecture, the data model and other useful things.

### Couchbase

See [`docs/couchbase.md`](docs/couchbase.md) for more on couchbase.
