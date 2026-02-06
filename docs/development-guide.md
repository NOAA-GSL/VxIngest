# Development Guide

This guide covers how to set up your environment to work on VxIngest. It also covers code standards, linting, and testing.

VxIngest is containerized for deployment. For more on using the application, see the [README.md](../README.md) in the repo root.

## Overview

VxIngest is a Python application, and uses [uv](https://docs.astral.sh/uv/) for dependency management. [Ruff](https://docs.astral.sh/ruff/) is used in the codebase for linting & formatting. The repo follows a ["`src`"-style](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/) [layout](https://www.pyopensci.org/python-package-guide/package-structure-code/python-package-structure.html).

VxIngest outputs Couchbase JSON documents to disk as part of the "ingest" process. Those are then imported separately by a bash script in `scripts/VXingest_utilities/run-import.sh`.

## Getting Started

You will first need to download and install [uv](https://docs.astral.sh/uv/getting-started/installation/). We will use uv to manage our Python venv's and dependencies.

uv can be used to run the application locally. Note you will need a `config.yaml` or `credentials` file with connection information for a Couchbase instance.

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
uv run ingest \
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
env DEBUG=true uv run ingest \
    -m tmp/output/metrics \
    -o tmp/output/out \
    -x tmp/output/xfer \
    -l tmp/output/log \
    -c config.yaml \ # this is the path to the file with your database credentials
    -j JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS
```

### An example of running the netcdf builder for METAR obs

```bash
uv run ingest  -m /tmp/output/metrics -o /tmp/output/out -x /tmp/output/xfer -l /tmp/output/log -c /Users/randy.pierce/adb-cb1-credentials -j JOB-TEST:V01:METAR:NETCDF:OBS -f 20250911_1500
```

## Developer tools

Linting, formatting, type checking, and unit testing can be done through uv like so:

```bash
# Lint
uv run ruff check .
# Format
uv run ruff format .
# Type check
uv run mypy src
# Unit test
CREDENTIALS=config.yaml uv run pytest tests
# Coverage report
CREDENTIALS=config.yaml uv run coverage run -m pytest tests && \
    uv run coverage report && \
    uv run coverage html
```

### Testing

You will need some data files downloaded locally in order to use the test suite. For more details, see [tests/vxingest/README.md](../tests/vxingest/README.md).

If you are using VSCode, the test suite should be picked up automatically. However, to set the `CREDENTIALS` env variable in VSCode, you will want to put the value in a `.env` file in the root of the repo like so:

`.env`:

```env
CREDENTIALS=config.yaml
```

### Container Build

Be aware there are different Dockerfiles in this repo - one for each service. This lets us keep our container images small and targeted to just the application that needs to be run.

#### Ingest

You can build the docker container with the following:

```bash
docker build \
    --build-arg BUILDVER=dev \
    --build-arg COMMITBRANCH=$(git branch --show-current) \
    --build-arg COMMITSHA=$(git rev-parse HEAD) \
    -f ./docker/ingest/Dockerfile \
    -t vxingest/ingest:dev \
    .
```

And run it via Docker Compose with the below. You'll need to update the `compose.yaml` file in the repo with your image tag. Note the `data` and `public` env variables point to where the input data resides and where you'd like the container to write out to. These are currently (12/2023) mounted to `/opt/data` inside the container.

```bash
data=/data-ingest/data \
    public=/public \
    docker compose run ingest 
```

Otherwise, note there are a number of targets in the Dockerfile. You can use the `--target=dev` flag to build a dev version. If you do so, and want to run tests, you'll need to update the `src` mount path below to where your test data is. If you're using Rancher Desktop, the data will need to be somewhere in your home directory in order to mount it in the container. You can run the container directly like so.

```bash
docker build \
    --target=dev \
    -f ./docker/ingest/Dockerfile \
    -t vxingest/ingest:test \
    .
docker run \
    --rm \
    --mount type=bind,src=$(pwd)/tmp/test-data/opt/data,dst=/opt/data \
    -it \
    vxingest/ingest:test \
    bash
CREDENTIALS=config.yaml uv run pytest tests
```

Or to build & run the prod version of the image, you can do the following to build:

```bash
docker build \
    -f ./docker/ingest/Dockerfile \
    -t vxingest/ingest:prod \
    .
```

And the following to run. Note that we're mounting the two things into the container - the directory we want to write output to (`$HOME/output`, mounted to `/opt/data`) and the credentials file we want to use `$(pwd)/config.yaml`. These both are relatively flexible. However, if you're running the NetCDF or the GRIB ingest you will also need to mount a directory containing those files on your local computer to the location specified in the import job doc in the database in the container. (Typically job docs specify `/public`)

```bash
docker run --rm \
    --env DEBUG=true \
    --mount type=bind,src=$HOME/output,dst=/opt/data \
    --mount type=bind,src=$(pwd)/config.yaml,dst=/app/config.yaml,readonly \
    vxingest:prod \
    -m /opt/data/metrics \
    -o /opt/data/out \
    -x /opt/data/xfer \
    -l /opt/data/log \
    -c /app/config.yaml \
    -j JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS
```

#### Import

```bash
docker build \
    -f docker/import/Dockerfile \
    --build-arg BUILDVER=dev \
    --build-arg COMMITBRANCH=$(git branch --show-current) \
    --build-arg COMMITSHA=$(git rev-parse HEAD) \
    -t vxingest/import:dev \
    .
```

You can run the "import" via Docker Compose like this example. You will need to use the same value for `data` as you used for the "ingest".

```bash
data=/data-ingest/data \
    docker compose run import
```

### Docker Compose

There is currently a Docker Compose file with options to run unit tests and ingest from within the container. This may be a useful option for local development as well.

*NOTE*: if you're using Rancher Desktop, you won't be able to access /opt on your system as it's not mounted into the VM by default. You'll need to move your test files into your home directory.

* `shell`: expects /data and /public for mounting
* `test`: expects /opt/data for mounting
* `ingest`: expects /data and /public for mounting
* `import`: expects /data for mounting

And can be run like:

```bash
data=/home/path/to/a/copy/of/opt/data docker compose run test
```

## Notes

### General

See [`docs/general-notes.md`](docs/general-notes.md) for a general overview of Architecture, the data model and other useful things.

### Couchbase

See [`docs/couchbase.md`](docs/couchbase.md) for more on couchbase.
