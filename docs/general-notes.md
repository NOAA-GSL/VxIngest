# General Notes

## Containers

The `scripts/cbtools` directory is included for use within Linux x86-64 containers. Do not expect those tools to work on other platforms or architectures. If you need a compatible copy, see <https://docs.couchbase.com/cloud/reference/command-line-tools.html>.

Credentials are passed into Compose services as secrets. By default, `compose.yaml` expects a credentials file at `${HOME}/credentials`, though you can override that with the `CREDENTIALS_FILE` environment variable.

The Compose file in this branch provides `shell`, `test`, and `ingest` services. The host directories mounted for those services are:

* `data` -> `/opt/data`
* `public` -> `/public`

Use `data` for all services, and `public` for the `ingest` and `shell` services.

## Utilities

The `scripts/` directory contains many administration, monitoring, backup, and troubleshooting helpers, plus a small set of VxIngest operational scripts under `scripts/VXingest_utilities/`.

## Running an ingest job service

To run a single ingest job, supply `data`, `public`, and the job identifier. The output, log, metrics, and transfer directories are mounted under `/opt/data` inside the container.

```bash
data=/data-ingest/data public=/public \
docker compose run ingest \
    -j JOB:V01:METAR:GRIB2:MODEL:HRRR
```

The service uses the credentials secret mounted at `/run/secrets/CREDENTIALS_FILE`. The `-j` flag identifies the job document to run. Optional flags such as `-f` are passed through to the specific builder selected by the job.

The ingest writes output documents and transfer tarballs into `/opt/data`. Any downstream import of those tarballs is handled outside this branch.

## Data model

The data model is best viewed with Hackolade. Refer to [model/docs/README.md](model/docs/README.md) for instructions on how to access the model.

## Architecture

See the [architecture overview diagram](https://docs.google.com/drawings/d/1eYlzZKAWOgKjuMVg6wVZHn0Me80TyMy5LQMUhNv-wWk/edit).

The design follows a [builder pattern](https://en.wikipedia.org/wiki/Builder_pattern). There is a top-level VXIngest class defined in `run_ingest_threads.py` that owns a thread pool of VXIngestManagers, a queue of input data, and the command-line interface to the factory. For a NetCDF builder the queue might be a queue of NetCDF files. For a CTC builder it might be a queue of ingest templates. Each VXIngestManager has an object pool of builders. Each builder uses an ingest template and a data source to create output documents. When the queue is depleted, the VXIngestManager writes documents into the configured output location.

## Builders

The basic plan is to have as many builders as there are fundamental data types. Initially there are:

* [../src/vxingest/netcdf_to_cb/README.md](../src/vxingest/netcdf_to_cb/README.md) - MADIS data from NetCDF files.
* [../src/vxingest/grib2_to_cb/README.md](../src/vxingest/grib2_to_cb/README.md) - model output data in GRIB files.
* [../src/vxingest/ctc_to_cb/README.md](../src/vxingest/ctc_to_cb/README.md) - contingency table data derived from observations and corresponding model output.

Each builder follows a factory pattern.
