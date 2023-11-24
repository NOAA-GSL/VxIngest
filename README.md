# VXXIngest

## Disclaimer

This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an “as is” basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.

## Purpose

The VXIngest project contains code for the purpose of ingesting meteorological data from various different sources into a document database. The data gnerated is in the form of JSON documents. These documents conform to the data model that is described in the model subdirectory.

## Build
NOTE: the scripts/cbtools are included for use within the container. These tools are linux x86-64 oriented. Do not expect them to work on other platforms or achitectures. If you need a compatible copy of these tools see [https://docs.couchbase.com/cloud/reference/command-line-tools.html](https://docs.couchbase.com/cloud/reference/command-line-tools.html)

NOTE: You can use ```docker system prune -af``` to clean up stopped, old or unused images from your machine. It recovers a lot of space.

The Dockerfile has two targets, dev and prod. With the dev target you get some development tools built into the container to facilitate debugging.

To build a docker image cd to the directory where VXingest was cloned
and check out the branch that you want to build, then to create an image ...

```bash
$ docker build \
    --build-arg BUILDVER=dev \
    --build-arg COMMITBRANCH=$(git branch --show-current) \
    --build-arg COMMITSHA=$(git rev-parse HEAD) \
    --target=prod \
    -t vxingest/development/:dev \
    .
```

Of course the --target=prod could also be --target=dev
or without the broken lines...

``` bash
docker build --build-arg BUILDVER=dev --build-arg COMMITBRANCH=$(git branch --show-current) --build-arg COMMITSHA=$(git rev-parse HEAD) --target=prod -t vxingest/development:dev .
```

Of course the --target=prod could also be --target=dev.

## Run

### Run the image in docker with a bash terminal

``` bash
docker run -it docker.io/vxingest/development:dev /bin/bash
```

Running like this allows you to exaamine the container.
You can run unit or int tests and debug problems with pdb if you create a credentials file and set the CREDENTIALS environment variable to point to that file.

### Run the image in docker with dockercompose

Docker compose takes care of most of the details of running unit or integration tests, the actual ingest process, the import process, or just a shell for debugging and examining the container.
You may have to init a swarm if you haven't already done that.

``` bash
docker swarm init
```

Credentials are passed in as a secret. To establish a CREDENTIALS secret you MUST have a credentials file in your home directory. A credentials file "${HOME}/credentials" looks like this except with valid credentials...

```@text
cb_host: ahost
cb_user: auser
cb_password: apwd
cb_bucket: vxdata
cb_scope: _default
cb_collection: METAR

```

The docker compose file expects a few directories to be available on the docker host (possibly your development platform)
depending on the service.
/opt/data has test data
/opt/data is a shared mounted directory that is used by ingest to store output documents and by import to read the output documents. It also has test data.
/public is usually the DSG /public that has all of GSL data in it. This is where the ingest processes find grib and netcdf files etc..
You must specify data for all services and both data and public for ingest and shell services.

#### services

  shell: expects /data and /public for mounting
  unit_test: expects /opt/data for mounting
  int_test: expects /opt/data for mounting
  ingest: expects /data and /public for mounting
  import: expects /data for mounting

#### invocations

These are single run services. To run a compose service do
"docker compose run service-name" like

```bash
data=/opt/data docker compose run unit_test

```

or

```bash
data=/opt/data docker compose run int_test
```

### Running the ingest service

To run an ingest there are a few extra parameters in addition to the /data directory. This is the typical invocation.

```bash
data=/data-ingest/data public=/public docker compose run ingest ./scripts/VXingest_utilities/run-ingest.sh -c /run/secrets/CREDENTIALS_FILE -o /opt/data/test/outdir -l /opt/data/test/logs -m /opt/data/test/metrics -x /opt/data/test/xfer"

```

The part after "run ingest" overrides the simple command in the service with all the necessary parameters. These parameters can be changed as required.

The ingest service will run all of the jobs that are currently scheduled (in the job documents) to run in the current fifteen minute interval i.e. quarter hour.

### Running an ingest job service

To run a single ingest job there are a few extra parameters in addition to the /data directory. directories should be under the /opt/data mountpoint (where the data path is mounted). This is a typical invocation.

```bash
 data=/data-ingest/data public=/public docker compose run ingest ./scripts/VXingest_utilities/run-ingest.sh -c /run/secrets/CREDENTIALS_FILE -o /opt/data/test/outdir -j JOB:V01:METAR:GRIB2:MODEL:HRRR -l /opt/data/test/logs -m /opt/data/test/metrics -x /opt/data/test/xfer -f 20329817000006"
```

Where -c is the internal credentials file passed as a secret. Don't change that path, but do be sure to have a "credentials" file in your 4{HOME}. The -l is where you want to store and archive log files, /data is the directory for temporarily storing output documents, -m is the directory where job metrics are stored to be collected, -j is a job document id, and -x is the directory where archived job results (documents and associated log files) are stored in expectation of being imported and scraped by an import process. Scraping is the process of gathering metrics from the log files. The arguments will be passed to the service through the environment. The ingest service will run all of the jobs that are currently scheduled (in the job documents) to run in the current fifteen minute interval i.e. quarter hour.

### Running an import job service

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

## data model

The data model is best viewed with Hackolade. Refer to [model](model/docs/README.md) for instructions on how to access the model.

## Architecture

The architecture is briefly outlined [Here](https://docs.google.com/drawings/d/1eYlzZKAWOgKjuMVg6wVZHn0Me80TyMy5LQMUhNv-wWk/edit).

The design follows a [builder pattern](https://en.wikipedia.org/wiki/Builder_pattern).
There is a top level VXIngest class defined in run_ingest_threads.py that has the reponsibility of owning a thread pool of a specified number of 'Director' role VXIngestManagers and a queue of input data, as well as providing the command line interface to the factory. For a netcdf_builder the queue might be a queue of netcdf files. For a ctc builder it might be a queue of ingest templates. Each VXIngestManager has an object pool of builders. Each builder will use an ingest template and a data source to create output documents. When the queue is depleted the VXIngestManager writes the documents into a specified location where they can easily be imported to the database.

## Output data file processing and data flow

This is a sample cron entry for the ingest process

```bash
*/15 * * * * /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-ingest.sh -c /home/amb-verif/adb-cb1-credentials -d /home/amb-verif/VxIngest -l /home/amb-verif/VxIngest/logs -o /data -m /data/common/job_metrics -x /data/temp > /home/amb-verif/logs/cron-ingest-`date +\%s`.out 2>&1
```

The ingest routines create data output files in an output directory tree, the root of which is specified by the "-o output directory" parameter. The actual subdir that a given builder writes into is determined by the builder type. Then, if the ingest is successful, the output data files are bundled and gzipped and moved to a data transfer directory that is specified by the "-x transfer directory" parameter. The data files themselves are moved to a "output_to_purge" directory within the output directory tree. This is so that the artifacts of the ingest are available for a period of time in case they need to be recovered or examined for errors. The ingest log file is bundled and transferred along with the data output files.

This is a sample cron entry for the import process.

```bash
*/2 * * * * /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-import.sh -c /home/amb-verif/adb-cb1-credentials -d /home/amb-verif/VxIngest -l /data/temp -m /data/common/job_metrics -t /data/temp_tar > /home/amb-verif/logs/cron-import-`date +\%s`.out 2>&1
```

At some point in time (currently in a cron job) the transfer tar files will be moved to the actual import load directory on the import server. The load directory is specified on the import job with the "-l load dir" parameter. It should be noted that the ingest transfer directory and the import load directory may very well be on different servers.

The import routine will unbundle any tar files found in the import load directory, one file at a time, and place the contents (the output files and the ingest log file) into a temporary directory tree the root of which is specified in the -t "temp_dir" parameter. The temporary directory is named after the PID of the import process.

After the import is completed, if the import was successful the tar file will be moved to a purge directory within the tempory directory tree. If the import was unseccessful the tar file will be moved to an archive directory within the tempory directory tree.

finally the purge directories are cleaned up, but only the purge data that is older than 3 days will be removed.

## Deployment

Ingest is currently deployed on adb-cb1.gsd.esrl.noaa.gov. All ingest happens to this Couchbase server and then the documents are replicated to the cluster which is composed of adb-cb2, adb-cb3, and adb-cb4.

The entire code base is in github in the VXIngest repository.
The run scripts rely on a python virtual environment.
The ingest is currently triggered by a crontab entry for the “amb-verif” user and that entry runs a run-cron.sh script that is in the scripts/VXingest_utilities directory. That script activates a python virtual environment with ‘source ${HOME}/vxingest-env/bin/activate’ and exports a PYTHONPATH environment to be the top level of the cloned repo. This same mechanism can be used to set up a suitable development environment on any computer with appropriate network connectivity, python3, and that has imported the packages in the requirements.txt file. Vscode extensions can be loaded with the vscode_extensions.sh script. This [readme](https://github.com/NOAA-GSL/VxIngest/blob/d9486f6576f0358db65df03ba9ac3da05fe64db8/grib2_to_cb/test/README.md#L267) has more explicit instructions on how to set up the virtual environment.

## Builders

The basic plan is to have as many builders as there are fundemental data types. Initially there are...

- netcdf - which is madis data from netcdf files. The source code for this is in netcdf_to_cb.
- grib2 - which is model output data in grib files. The source code for this is in grib2_to_cb.
- ctc - which is contigency table data derived from observations (netcdf data) and corresponding model output data. The source code for this is in ctc_to_cb.

Each builder follows a factory pattern.

Each builder directory has a readme with more extensive information. The intent is to make the builders standalone.

### [netcdf_builder](netcdf_to_cb/README.md)

### [grib2_builder](grib2_to_cb/README.md)

### [ctc_builder](ctc_to_cb/README.md)

## Metrics

Metrics are generated by a combination of log messages and a scraper utility script in scripts/VXingest_utilities/scrape_metrics.sh. The scraper utility script is called from the import routine.

Metrics are written to a collection directory where they are picked up from a text file collector configured on the node_exporter service for the node where the scrape_metrics.sh script is run.

Currently scrape_metrics.sh is called from run-imports.sh which is called from a cron entry.
Refer to the vx-prometheus project for more information.

## Notes on Couchbase indexes, general utilities, and configuration

### indexes

Indexes are what makes a document database work. We maintain an primary index on the standalone server but not on the cluster as primary indexes are very expensive. Indexes are sort of living. As we develop ingest code and MATS app code we will be adding new indexes to meet new needs.

#### standard utilites from Couchbase class

There are a lot of administrative scripts for dealing with indexes in the
...scripts/admin/index directory. Full-text-search index utilities are in the
...scripts/admin/fts directory.
This utility can be used to retrieve all the currently defined indexes from a cluster.
This invocation filters for the mdata bucket.
.../VXingest/scripts/admin/index/index-definitions.sh --cluster=adb-cb4.gsd.esrl.noaa.gov --username=avid --password='pwd' | grep mdata
(the password is not actual)

The create...n1ql files in the [index_creation_scripts](https://github.com/NOAA-GSL/VxIngest/blob/c17e1900a3f40f0ac6d84af92e42556498d4eced/mats_metadata_and_indexes/index_creation_scripts) folder were created with this utility.

There are index creation n1ql scripts in this directory. The scripts labeled ...-0-relicas.n1ql
will not produce replicas, i.e. use them on a standalone server, and the scripts labeled
...-2-replicas.n1ql should be used for the cluster.

They can be loaded with the ....scripts/admin/index/index-import-all-indexes.sh
script.
e.g.
.../VXingest/scripts/admin/index/index-import-all-indexes.sh --cluster=adb-cb4.gsd.esrl.noaa.gov --username=avid --password='pwd' --file=.../mats_metadata_and_indexes/index_creation_scripts/create_indexes-2-replicas.n1ql
(the password is not actual)

Alternatively you can load the indexes in the query console of the UI and then
use this command to actually build the indexes.

``` sh
BUILD INDEX ON mdata (( SELECT RAW name FROM system:indexes WHERE keyspace_id = "mdata" AND state = 'deferred' ));
```

## Useful and interesting queries

- This [page](https://docs.couchbase.com/server/current/fts/fts-geospatial-queries.html) talks about geospatial queries.
- This [page](https://docs.couchbase.com/server/current/fts/fts-searching-from-the-ui.html) talks about full text searches from the UI. Note that UI FTS searches are pretty limited.
- This [page](https://docs.couchbase.com/server/current/fts/fts-searching-with-the-rest-api.html)] talks about using curl.

### Important note about OUR N1QL queries and indexes

Each N1QL query requires an index to work. Our basic indexes
cover the type, docType, version, and subset fields.
That means, for our case, that you must have

``` sql
WHERE type="DD"
and docType = "station"
and subset = "METAR"
and version = "V01"
```

in each of your N1QL queries.

### N1QL queries

This query returns all the metadata id's and updated time.

``` sql
select updated, meta().id from mdata where type="MD" and docType is not missing and version = "V01"  and subset="COMMON"
```

This query modifies the above to only return for the models "HRRR" and "HRRR_OPS".

``` sql
select meta().id, updated from mdata where type="MD" and docType is not missing and version = "V01"  and subset="COMMON" and model in ["HRRR", "HRRR_OPS"];
```

This query returns the minimum fcstValidBeg and the maximum
fcstValidBeg for all the METAR obs in the mdata bucket.

``` sql
select min(mdata.fcstValidBeg) as min_fcstValidBeg, max(mdata.fcstValidBeg) as max_fcstValidBeg
from mdata
WHERE type="DD"
and docType = "obs"
and subset = "METAR"
and version is not missing
```

This is the same thing but with epochs, which is useful for setting
parameters for ingest.

``` sql
select min(mdata.fcstValidEpoch) as min_fcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch
from mdata
WHERE type="DD"
and docType = "obs"
and subset = "METAR"
and version is not missing
```

This query will return a lot of results without further filtering in the predicates.

``` sql
  select raw meta().id from mdata
  where type="DD" and
  docType="obs"
  and subset="METAR"
  and version="V01"
  limit 100
  ```

This query will use N1QL to perform a basic geospatial query, it assumes that the
full text search index for stations has been loaded.
That index creation script is in
```VXingest/gsd_sql_to_cb/index_creation_scripts```.

This query should find the "PORT_MORESBY_INTL" station.

``` sql
select raw station
from mdata as station
WHERE type="DD"
and docType = "station"
and subset = "METAR"
and version = "V01"
and SEARCH(station,
{
"location": {"lat": -9.4286, "lon": 147.2198},
"distance": "100ft",
"field": "geo"
}
)
```

This is a search by station name query and should return DIA.

``` sql
select raw station
from mdata as station
WHERE type="DD"
and docType = "station"
and subset = "METAR"
and version = "V01"
and SEARCH(station.name,"KDEN")
```

This is another way to do the above query.

``` sql
select raw station
from mdata as station
WHERE type="DD"
and docType = "station"
and subset = "METAR"
and version = "V01"
and SEARCH(station,{"field":"name", "match": "kden"})
```

This is a N1QL search by partial description, using a regular expression
against the description field. It should return DIA.

``` sql
select raw station
from mdata as station
WHERE type="DD"
and docType = "station"
and subset = "METAR"
and version = "V01"
and SEARCH(station,{"field":"description", "regexp": "denver.*"})
```

### N1QL metadata queries

These are mostly oriented around cb-ceiling but they are illustrative for all documents.
This will return the min and max fcstValidEpoch for the HRRR model

``` sql
select min(mdata.fcstValidEpoch) as mindate, max(fcstValidEpoch) as maxdate from mdata where type="DD" and docType="model" and subset="METAR" and version="V01" and model='HRRR';
```

Alternatively the min and max fcstValidEpochs can be returned quickly with

``` sql
select min(meta().id) as minid, max(meta().id) as maxid from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```

because the ids contain the fcstValidEpochs and the ids will sort by them.
This returns the nuber of METAR contingency tables for the HRRR
select count(meta().id) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
numrecs: 1248123,

This returns the current epoch
```select floor(NOW_MILLIS()/1000)```

This returns the distinct array of regions for the contingency tables for the HRRR

``` sql
select raw array_agg(distinct mdata.region) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```

This returns the distinct array of forecast lengths for the contingency tables for the HRRR

``` sql
select raw array_agg(distinct mdata.fcstLen) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```

This returns the distinct array of thresholds for the contingency tables for the HRRR. This takes a pretty long time,
around a minute.

``` sql
SELECT DISTINCT RAW d_thresholds
FROM (SELECT OBJECT_NAMES (mdata.data) AS thresholds
      FROM  mdata 
      WHERE  type="DD" AND docType="CTC" AND  subset="METAR" AND  version="V01" AND model='HRRR')  AS d
UNNEST d.thresholds AS d_thresholds;
```

## Useful curl queries

Curl queries can be implemented on the command line or in the client SDK.
This is an example of doing a regular expression query for the word "denver" (case insensitive because of the search index analyzer) at the front of any description. The results are piped into jq to make them pretty.
The password is fake so replace it with the actual password.

- This is the N1QL search mentioned above for returning the minimum fcstValidBeg
for all the METAR obs in the mdata bucket, executed with the curl rest api.

``` sh
curl -s -u 'avid:getapassword' http://adb-cb4.gsd.esrl.noaa.gov:8093/query/service  -d 'statement=select min(mdata.fcstValidEpoch) as min_fcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type="DD" and docType = "obs" and subset = "METAR" and version is not missing' | jq -r '.results | .[] | .min_fcstValidEpoch'
```

This is the same but it returns the max fcstValidBeg

``` sh
curl -s -u 'avid:getapassword' http://adb-cb4.gsd.esrl.noaa.gov:8093/query/service  -d 'statement=select min(mdata.fcstValidEpoch) as min_fcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type="DD" and docType = "obs" and subset = "METAR" and version is not missing' | jq -r '.results | .[] | .max_fcstValidEpoch'
```

- This returns a hit list with one hit for DIA.

``` sh
curl -XPOST -H "Content-Type: application/json" -u 'avid:fakepassword' http://adb-cb4.gsd.esrl.noaa.gov:8094/api/index/station_geo/query -d '{"fields": ["*"],"query": {"fields":["*"], "regexp": "^denver.*","field":"description"}}' | jq '.'
```

This is a curl command that searches by lat and lon for stations within 1 mile of 39.86, -104.67 and it finds DIA

``` sh
curl -XPOST -H "Content-Type: application/json" -u 'avid:fakepassword' http://adb-cb4.gsd.esrl.noaa.gov:8094/api/index/station_geo/query -d '{"fields": ["*"],"query":{"location":{"lat":39.86,"lon":-104.67},"distance":"1mi","field":"geo"}}' | jq '.'
```

It completes in under 40 milliseconds.

This command looks for all the stations within an arbitrary polygon that I drew on google maps,
maybe about a third of the country somewhere in the west...

```curl -XPOST -H "Content-Type: application/json" -u 'avid:fakepassword' http://adb-cb4.gsd.esrl.noaa.gov:8094/api/index/station_geo/query -d '{"fields": ["*"],"query":{"polygon_points":["47.69065526395918, -120.699049630136","44.97376705258397, -91.33055527950087","36.68188062186998, -92.26638359058016","37.13420293523954, -114.52912609347626"]},"field":"geo"}' | jq '.'```

It returns 148 stations in under half a second.

## Useful utilities

There is a scripts directory, much of which came from Couchbase training.
This directory contains many useful scripts for administration, monitoring, and accessing Couchbase statistics.

## Useful search predicates for retrieving documents

To retrive all the ingest documents for METARS

``` sh
type="MD" and docType="ingest" and subset="METAR" and version="V01"
```

To retrieve all the ingest documents for METARS and restrict it to only CTC ingest documents.

``` sh
type="MD" and docType="ingest" and subset="METAR" and version="V01" and subType="CTC"
```

To retrieve all the CTC documents for METARS and model HRRR

``` sh
type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR"
```

To retrieve 10 CTC documents for HRRR METARS

``` sql
select mdata.* from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR" limit 10
```

To retrieve 10 CTC documents for HRRR METARS at a specific fcstValidEpoch

``` sql
select mdata.* from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR" and fcstValidEpoch=1516986000 limit 10
```

To count all the METAR model documents where the model is HRR_OPS
If you retrieve these documents and examine them you find that these documents have model
variables at specific fcstValidBeg and are organized by metar station.

``` sql
select count(*) from mdata where type="DD" and docType="model" and subset="METAR" and version="V01" and model="HRRR_OPS"
```

To retrieve the metadata document for cb-ceiling for the HRRR model

``` sql
select mdata.* from mdata where type="MD" and docType="mats_gui" and subType="app" and subset="COMMON" and version="V01" and app="cb-ceiling" and model="HRRR"
```

To retrieve the metadata document for cb-ceiling for all models

``` sql
select mdata.* from mdata where type="MD" and docType="mats_gui" and subType="app" and subset="COMMON" and version="V01" and app="cb-ceiling"
```

## Initial configuration recommendations

For both the single server and the three node cluster it is most advisable to
run the Query, Index, and Data services on all the nodes.
With the single node server there are no replications possible, but for
the cluster we should start with num_recs = 2 (one less than the number of nodes) which
will result in three instances of each service.

We must change the default data path to a large
