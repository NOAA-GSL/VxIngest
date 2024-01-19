# General Notes

## Containers

The `scripts/cbtools` directory is included for use within the container. These tools are linux x86-64 oriented. Do not expect them to work on other platforms or achitectures. If you need a compatible copy of these tools see [https://docs.couchbase.com/cloud/reference/command-line-tools.html](https://docs.couchbase.com/cloud/reference/command-line-tools.html)

Credentials are passed in as a secret. To establish a CREDENTIALS secret you MUST have a credentials file in your home directory.

The docker compose file expects a few directories to be available on the docker host (possibly your development platform) depending on the service.

* `/opt/data` has test data
* `/opt/data` is a shared mounted directory that is used by `vxingest` to store output documents and by `run-import.sh` to read the output documents. It also has test data.
* `/public` is usually the DSG /public that has all of GSL data in it. This is where the ingest processes find grib and netcdf files etc..
* You must specify data for all services and both data and public for ingest and shell services.

## Utilities

There is a scripts directory, much of which came from Couchbase training.
This directory contains many useful scripts for administration, monitoring, and accessing Couchbase statistics.

## Running an import job service

data=/data-ingest/data docker compose run import ./scripts/VXingest_utilities/run-import.sh -c /run/secrets/CREDENTIALS_FILE -l load directory -t temp_dir -m metrics_directory

The parameters are very similar to the ingest service.
These directory parameters should be within the supplied data mountpoint relative to the /opt/data mountpoint.

- The credentials-file specifies cb_host, cb_user, and cb_password.
- The load directory is where the program will look for the tar files
- The temp_dir directory is where the program will unbundle the tar files (in uniq temporary subdirs)
- The metrics directory is where the scraper will place the metrics

for example:

```bash
data=/data-ingest/data public=/public docker compose run import ./scripts/VXingest_utilities/run-import.sh -c /run/secrets/CREDENTIALS_FILE -l /opt/data/xfer -t /opt/data/temp_tar -m /opt/data/common/job_metrics
```

## Running an ingest job service

To run a single ingest job there are a few extra parameters in addition to the /data directory. directories should be under the /opt/data mountpoint (where the data path is mounted). This is a typical invocation.

```bash
 data=/data-ingest/data public=/public docker compose run ingest ./scripts/VXingest_utilities/run-ingest.sh -c /run/secrets/CREDENTIALS_FILE -o /opt/data/test/outdir -j JOB:V01:METAR:GRIB2:MODEL:HRRR -l /opt/data/test/logs -m /opt/data/test/metrics -x /opt/data/test/xfer -f 20329817000006"
```

Where -c is the internal credentials file passed as a secret. Don't change that path, but do be sure to have a "credentials" file in your ${HOME}. The -l is where you want to store and archive log files, -m is the directory where job metrics are stored to be collected, -j is a job document id, -f is an optional file pattern that can be used to qualify the input files (the job document will specify the input data path), and -x is the directory where archived job results (documents and associated log files) are stored in expectation of being imported and scraped by an import process. Scraping is the process of gathering metrics from the log files. The arguments will be passed to the service through the environment. The ingest service will run all of the jobs that are currently scheduled (in the job documents) to run in the current fifteen minute interval i.e. quarter hour.

The -f flag in the example is specific to a GRIB2 JOB. A netcdf JOB would something like 20231124_1500. A CTC or a SUM job would have different parameters i.e. -f first_epoch and -l last_epoch. These optional parameters are passed through to the particular builder.

## Data model

The data model is best viewed with Hackolade. Refer to [model](docs/model/docs/README.md) for instructions on how to access the model.

## Architecture

The architecture is briefly outlined [Here](https://docs.google.com/drawings/d/1eYlzZKAWOgKjuMVg6wVZHn0Me80TyMy5LQMUhNv-wWk/edit).

The design follows a [builder pattern](https://en.wikipedia.org/wiki/Builder_pattern).
There is a top level VXIngest class defined in run_ingest_threads.py that has the reponsibility of owning a thread pool of a specified number of 'Director' role VXIngestManagers and a queue of input data, as well as providing the command line interface to the factory. For a netcdf_builder the queue might be a queue of netcdf files. For a ctc builder it might be a queue of ingest templates. Each VXIngestManager has an object pool of builders. Each builder will use an ingest template and a data source to create output documents. When the queue is depleted the VXIngestManager writes the documents into a specified location where they can easily be imported to the database.

## Builders

The basic plan is to have as many builders as there are fundemental data types. Initially there are...

- [netcdf](src/vxingest/netcdf_to_cb/README.md) - which is madis data from netcdf files. The source code for this is in netcdf_to_cb.
- [grib2](src/vxingest/grib2_to_cb/README.md) - which is model output data in grib files. The source code for this is in grib2_to_cb.
- [ctc](src/vxingest/ctc_to_cb/README.md) - which is contigency table data derived from observations (netcdf data) and corresponding model output data. The source code for this is in ctc_to_cb.

Each builder follows a factory pattern.