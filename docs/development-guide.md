# Development Guide

This guide covers how to set up your environment to work on VxIngest. It also covers linting, formatting, testing, and container-based development.

VxIngest is containerized for deployment. For general usage, see [../README.md](../README.md).

## Overview

VxIngest is a Python application that uses [uv](https://docs.astral.sh/uv/) for dependency management and command execution. [Ruff](https://docs.astral.sh/ruff/) is used for linting and formatting. The repo follows a [`src` layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/).

The ingest writes Couchbase-ready JSON documents, logs, metrics, and transfer tarballs to disk. The transfer tarballs are retained for downstream consumers, but the downstream import runtime is not maintained in this repo.

## Getting Started

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and initialize the development environment:

```bash
uv sync --locked --dev
```

You will need a `config.yaml` or similar credentials file with Couchbase connection information.

Example `config.yaml`:

```yaml
cb_host: "url.for.couchbase"
cb_user: "user"
cb_password: "password"
cb_bucket: "vxdata"
cb_scope: "_default"
cb_collection: "METAR"
```

Run the application locally:

```bash
mkdir -p tmp/output/{metrics,out,xfer,log}
uv run ingest \
    -m tmp/output/metrics \
    -o tmp/output/out \
    -x tmp/output/xfer \
    -l tmp/output/log \
    -c config.yaml \
    -j JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS
```

For debug output, set `LOG_LEVEL` to `DEBUG`. The supported values are `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`; when unset, VxIngest uses `INFO`.

```bash
mkdir -p tmp/output/{metrics,out,xfer,log}
LOG_LEVEL=DEBUG uv run ingest \
    -m tmp/output/metrics \
    -o tmp/output/out \
    -x tmp/output/xfer \
    -l tmp/output/log \
    -c config.yaml \
    -j JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS
```

### Testing mode

To run ingest in testing mode, set the `TESTING` environment variable (any value). When set, the ingest will process both status='active' and status='test' job documents. When not set, only status='active' documents are processed. This allows test documents to be safely developed and tested without risk of automatic runners (like cron) inadvertently executing them.

```bash
mkdir -p tmp/output/{metrics,out,xfer,log}
TESTING=1 uv run ingest \
    -m tmp/output/metrics \
    -o tmp/output/out \
    -x tmp/output/xfer \
    -l tmp/output/log \
    -c config.yaml \
    -j JOB-TEST:V01:METAR:NETCDF:OBS
```

Example NetCDF METAR run:

```bash
uv run ingest \
    -m /tmp/output/metrics \
    -o /tmp/output/out \
    -x /tmp/output/xfer \
    -l /tmp/output/log \
    -c /path/to/credentials \
    -j JOB-TEST:V01:METAR:NETCDF:OBS \
    -f 20250911_1500
```

## Developer tools

Common commands:

```bash
# Lint
uv run ruff check .

# Format
uv run ruff format .

# Type check
uv run mypy src

# Full test suite
CREDENTIALS=config.yaml uv run pytest tests

# Fast path without integration tests
CREDENTIALS=config.yaml uv run pytest -m "not integration" tests

# Coverage
CREDENTIALS=config.yaml uv run coverage run -m pytest tests && \
    uv run coverage report && \
    uv run coverage html
```

### Pre-commit

If you want to use the pre-commit hooks:

1. Install pre-commit:

```console
uv tool install pre-commit --with pre-commit-uv --force-reinstall
```

1. Install the hooks:

```console
pre-commit install
```

Update hooks with `pre-commit autoupdate`.

### Testing

Some tests require local data files and a working Couchbase connection. See [../tests/vxingest/README.md](../tests/vxingest/README.md) for details.

If you are using VS Code, add a `.env` file in the repo root so the editor picks up the credentials path:

```env
CREDENTIALS=config.yaml
```

## Container Build

The repository uses a single multi-stage Dockerfile.

Build the standard ingest image:

```bash
docker build \
    --build-arg BUILDVER=dev \
    --build-arg COMMITBRANCH=$(git branch --show-current) \
    --build-arg COMMITSHA=$(git rev-parse HEAD) \
    -f ./docker/Dockerfile \
    -t vxingest/ingest:dev \
    .
```

Run it via Docker Compose:

```bash
data=/data-ingest/data \
public=/public \
docker compose run ingest -j JOB-TEST:V01:METAR:NETCDF:OBS
```

Build the `dev` target for testing and debugging:

```bash
docker build \
    --target=dev \
    -f ./docker/Dockerfile \
    -t vxingest/ingest:test \
    .
docker run \
    --rm \
    --mount type=bind,src=$(pwd)/tmp/test-data/opt/data,dst=/opt/data \
    -it \
    vxingest/ingest:test \
    bash
```

Inside that shell, you can run commands such as:

```bash
CREDENTIALS=config.yaml uv run pytest tests
```

Build the production image directly:

```bash
docker build \
    -f ./docker/Dockerfile \
    -t vxingest:prod \
    .
```

Run it directly with bind mounts:

```bash
docker run --rm \
    --env LOG_LEVEL=DEBUG \
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

## Docker Compose

The Compose file supports three development workflows in this branch:

- `shell`: interactive debugging shell
- `test`: test runner based on the Dockerfile `dev` target
- `ingest`: main ingest runtime

If you're using Rancher Desktop, you will need your mounted data under your home directory rather than under `/opt` on the host.

Example:

```bash
data=/home/path/to/a/copy/of/opt/data docker compose run test
```

## Notes

### General

See [general-notes.md](general-notes.md) for a general overview of architecture, the data model, and other useful details.

### Couchbase

See [couchbase.md](couchbase.md) for more on Couchbase.
