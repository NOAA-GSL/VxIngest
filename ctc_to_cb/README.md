# ctc ingest to couchbase

## purpose

This builder is intended to import ctc data into Couchbase taking advantage of the GSL Couchbase data schema that has been developed by the GSL AVID model verification team.
The ctc_builder.py program provides a CTCBuilder class that will read existing model and observation data and, using the formula above, create CTC documents that can be imported into the database.

## How CTC tables are derived

ARRAY_SUM(ARRAY CASE WHEN (pair.modelValue < 300
        AND pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS hits,
ARRAY_SUM(ARRAY CASE WHEN (pair.modelValue < 300
        AND NOT pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS false_alarms,
ARRAY_SUM(ARRAY CASE WHEN (NOT pair.modelValue < 300
        AND pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS misses,
ARRAY_SUM(ARRAY CASE WHEN (NOT pair.modelValue < 300
        AND NOT pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS correct_negatives

## Environment

These programs require python3, and couchbase sdk 3.0 minimum (see [couchbase sdk](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html) )

In the test directory [README](test/README.md) you will find instructions for setting up the environment and for running the tests.

## Approach

These programs use a load_spec YAML file to define which ingest templates are to be used, a credentials file to provide database authentication, command line parameters for run time options, and the associated ingest template documents from the database that are specified in the load_spec.yaml.

### load_spec example

This is the  ctc_to_cb/test/load_spec_metar_ctc_V01.yaml file from this distribution.

```json
load_spec:
  email: "randy.pierce@noaa.gov"
  ingest_document_ids: ["MD-TEST:V01:METAR:HRRR_OPS:ALL_HRRR:CTC:CEILING:ingest"]
```

The email is optional - currently not used.
The ingest_document_ids: ['MD-TEST:V01:METAR:HRRR_OPS:ALL_HRRR:CTC:CEILING:ingest'] line defines
one or a list of metadata documents. These documents define how the program will operate.
The 'MD-TEST:V01:METAR:HRRR_OPS:ALL_HRRR:CTC:CEILING:ingest' value is the id of a couchbase metadata ingest document.
This document MUST exist on the couchbase cluster defined by cb_host
and MUST be readable by the cb_user. Copies of the metadata documents are checked into
.../VXingest/mats_metadata_and_indexes/ingest_ctcs.json and there is a script
.../VXingest/mats_metadata_and_indexes/ingest_ctcs.sh that is usefull for importing
the associated metadata documents.

## ingest documents

[obs ingest documents](https://github.com/NOAA-GSL/VxIngest/blob/0edaa03be13d75812e19ecf295e952b46d255b8f/mats_metadata_and_indexes/metadata_files/ingest_stations_and_obs_netcdf.json)

## Builder class

The builder is [CTCModelObsBuilderV01](https://github.com/NOAA-GSL/VxIngest/blob/d9486f6576f0358db65df03ba9ac3da05fe64db8/ctc_to_cb/ctc_builder.py)

There is a base CTCBuilder which has the generic code for reading a pair of documents and generating model obs contingency pairs and then there is a specialized CTCModelObsBuilderV01 that inherits from the generic class. Specific information for these classes is available in the docstring for each class.

## Credentials files

This is an example credentials file, the user and password are fake.

``` yaml
  cb_host: adb-cb1.gsd.esrl.noaa.gov
  cb_user: a_gsd_user
  cb_password: A_gsd_user_password

```

## ingest documents - metadata

Refer to [ingest documents and metadata](https://github.com/NOAA-GSL/VxIngest/blob/77b73babf031a19ba9623a7fed60de3583c9475b/mats_metadata_and_indexes/metadata_files/README.md#L11)

## Tests

There are tests in the test directory. To run the test_write_load_job_to_files test
for example cd to the VXingest directory and use this invocation.
This assumes that you have cloned this repo into your home directory.

``` sh
source ~/VXingest/test_venv/bin/activate
export PYTHONPATH=~/VxIngest
python3 -m pytest ctc_to_cb/test/test_unit_metar_ctc.py::TestCTCBuilderV01Unit::tetest_write_load_job_to_files
```

## Examples of running the ingest programs

### run_cron.sh

The current ingest invocations are contained in the [run_cron.sh](https://github.com/NOAA-GSL/VxIngest/blob/main/scripts/VXingest_utilities/run-cron.sh)

``` sh
outdir="/data/ctc_to_cb/output/${pid}"
mkdir $outdir
python ${clonedir}/ctc_to_cb/run_ingest_threads.py -s /data/ctc_to_cb/load_specs/load_spec_metar_ctc_V01.yaml  -c ~/adb-cb1-credentials -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs
```

### ingest parameters

-s is the load_spec
-c credential file
-o is the directory where the output file documents will be placed
-t is the number of threads that the process will use

Each ingest process writes files to an output directory and then the generated document files are imported with the
[import_docs.sh](../scripts/VXingest_utilities/import_docs.sh utility)

### import parameters

-c credential file
-p the document directory (where the ingest process put its output fioes)
-n number of import processes to use
-l the log directory (each import process will create a temporary directory and then copy its logs to the log dir when it is finished importing)

### metadata Example

This is the contents of "MD:V01:METAR:HRRR_OPS:E_US:CTC:CEILING:ingest". You must either
be certain that it already exists in the couchbase cluster or you must create it.

```json
  {
    "id": "MD:V01:METAR:HRRR_OPS:E_US:CTC:CEILING:ingest",
    "builder_type": "CTCModelObsBuilderV01",
    "docType": "ingest",
    "type": "MD",
    "version": "V01",
    "subset": "METAR",
    "subType": "CTC",
    "subDocType": "CEILING",
    "model": "HRRR_OPS",
    "region": "E_US",
    "template": {
      "dataFileId": "DF_id",
      "dataSourceId": "DS_id",
      "id": "DD:V01:METAR:HRRR_OPS:E_US:CTC:CEILING:&handle_time:&handle_fcst_len",
      "type": "DD",
      "docType": "CTC",
      "subDocType": "CEILING",
      "model": "HRRR_OPS",
      "region": "E_US",
      "version": "V01",
      "subset": "METAR",
      "fcstValidISO": "&handle_iso_time",
      "fcstValidEpoch": "&handle_time",
      "fcstLen": "&handle_fcst_len",
      "data": "&handle_data"
    }
  },
```

The line
```"builder_type": "CTCModelObsBuilderV01"```
refers to a python class. This builder class is defined
in the ctc_builder.py file. This class will interpret the
load_spec and ingest data from a set of model and observation documents to create the ctc documents.
Notice

```code
    "type": "MD",
    "version": "V01",
    "docType": "ingest",
    "subset": "METAR",
```

#### field substitution by function in the template

If in the template above the line ```"&handle_time" or "&handle_fcst_len"```
defines a field substitution by named function. A named function must
exist in the specified builder class. These functions have a signature like

```code
def ceiling_transform(params_dict):
...
```

The named function routine processes a named function entry from a template.
The '_named_function_def' looks like "&named_function|*field1,*field2,*field3..."
where named_function is the literal function name of a defined function.
The name of the function and the function parameters are seperated by a "|" and
the parameters are seperated by a ','.
It is expected that field1, field2, and field3 etc are all valid variable names.
Each field will be translated from the grib2 file into an array of
tuples, one per station location. Each tuple will have a location precise value and an interpolated value.
It is up to the handler_function to decide which part of the tuple is appropriate.
The method "named_function" will be called like...
named_function({field1:[(value, interp_value),(value, interp_value)...], [(value, interp_value),(value, interp_value)...], ... }) and the return value from named_function will be substituted into the generated document.

In this example the function names are &handle_time and &handle_fcst_len and they are concatenated like &handle_time:&handle_fcst_len. The resulting string will look something like "1636466400:0".
The entire id line will therefore be something like "id": "DD:V01:METAR:HRRR_OPS:E_US:CTC:CEILING:1636466400:0".

#### Where to place substitutions

Substitutions can be for keys or values in the template, in top level documents or in sub documents.

## Structure of templates

Templates are given document identifiers like
```"MD:V01:METAR:HRRR_OPS:E_US:CTC:CEILING:ingest"```
This identifier is constrained to match specific fields within the
document. "type:version:subset:model:region:product:subdocType

and MUST contain these keywords...

 ```code
  "type": "MD",  - required to be 'MD'
  "docType": "ingest",  - required to be 'ingest'
  "subset": "METAR",  - required set to whatever is appropriate
  "version": "V01",  - the version of the template
  "builder_type": "some builder class"

```

## Backup ingest documents

Ingest documents can be backed up with a utility in the scripts/VX_ingest_utilities
directory... save_ingest_docs_to_csv.sh
This utility requires a backup directory which is nominally
VXingest/gsd_sql_to_cb/ingest_backup, and a server name. The utility will backup all the currently defined ingest documents
based on the id pattern "MD.*:ingest".

Alternatively a builder developer my choose to check the ingest documents into a code repository along with the
builder code, and provide a script for importing the documents into couchbase.

### Example

``` bash
${HOME}/VXingest/scripts/VXingest_utilities/save_ingest_docs_to_csv.sh ${HOME}/VXingest/gsd_sql_to_cb/ingest_backup adb-cb1.gsd.esrl.noaa.gov
```

#### Alternatively for personal backups

you can use the document export and import utility on the couchbase UI IF THE COUCHBASE
SERVER VERSION IS GREATER THAN 6.5.

To use the UI navigate to the UI Query page

<https://adb-cb4.gsd.esrl.noaa.gov:18091/ui/index.html#!/query>

Enter this query into the query editor and execute the query.

```code
select meta().id, ingest_docs.*
from mdata as ingest_docs
WHERE type="MD"
and docType = "ingest"
and subset = "METAR"
and version is not missing
```

This will retrieve all the ingest documents for the subset 'METAR' and
associate an id field with each document.

Click 'EXPORT' at the top right of the page, select the 'current query results (JSON)'
radio button, enter a file name, do not include a path. Whatever file you specify will
show up in your Downloads directory. If you specify a path
the path will get munged into the filename. The save button will save all the ingest documents to
the file that you specified IN THE DOWNLOADS DIRECTORY.

##### Restore ingest documents on local server

Ingest documents can be restored from the documents page IF YOU HAVE ADMINISTRATOR privileges
and the server VERSION IS 6.6 OR GREATER.
This is useful to restore ingest documents to your laptop.

Login wih administrator privileges. The go to the documents page...
```http://localhost:8091/ui/index.html#!/documents/import?scenarioZoom=minute```

There should be an 'Import Documents' link at the top, click that.
Choose the file that you previously saved. On the import panel make sure
that the 'Parse File As' selector is set to 'JSON List'.
Choose the destination bucket, for us it is usually 'mdata'. For the
'Import With Document ID' choose the 'Value of Field' radio button.
From the 'Value of Field:' selector choose 'id'.

Click the 'Import Data' button.

Your ingest documents should now be available.

### Restore ingest documents on local server using cbimports utility

If the version is less than 6.6 or you want to script loading jason documents you can use the cbimports utility.
The json documents must be in the form of a json list and each document must
have an 'id' field with a unique value. The 'id' value should reflect the identifiers in our data model.

#### Example restore ingest with cbimports

```code
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getapassword' --format list --generate-key %id% --dataset file:///${HOME}/VXingest/ctc_to_cb/ingest_backup/ingest-20210313:083606

```

#### Restore other metadata with cbimports

Refer to the VXingest/gsd_sql_to_cb/metadata_files/regions.json for
an example of a multi-document metadata file. The cbimport command for importing these region definitions
into a server would look like this for the server adb-cb4.gsd.esrl.noaa.gov
The password has been obscured and the example assumes that you cloned this repo into ${HOME}....

```code
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getyourselfapassword' --format list --generate-key %id% --dataset file:///${HOME}/VXingest/ctc_to_cb/metadata_files/regions.json
```

For more information on cbimport see [cbimport](https://docs.couchbase.com/server/current/tools/cbimport-json.html)

#### Import data with cbimports

In a similar manner to importing metadata, data files may be imported as well. This example
would be for a datafile containing a document array i.e a data file that is constructed like this...

```json
[
{
    id:"DD:V01:METAR:something:anepoch",
    more:"stuff"
},
{
    id:"DD:V01:METAR:something:anepoch",
    more:stuff"
},
more documents ,,,,

]
```

The id's must conform to our data model and must be unique.
The import command would be like this for importing to the cluster...

```bash
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getyourselfapassword' --format list --generate-key %id% --dataset file:///path_to_the_file
```

and to use multiple threads and log any errors you can do this, you should not use more threads than you have cpu cores....

```bash
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getyourselfapassword' --format list --generate-key %id% --dataset file:///path_to_the_file -t num_threads -e an_error_file
```

A strategy might be to seperate data into multiple files and run a different cbimport
instance on each file.

## Credentials files

This is an example credentials file, the user and password are fake.

```json
  cb_host: adb-cb1.gsd.esrl.noaa.gov
  cb_user: a_gsd_user
  cb_password: A_gsd_user_password

```

You must have both of these files and you must supply them to
the program as a parameter.

## Example invocation

This assumes that you have cloned this repo into your home directory.

```bash
cd ~/VXingest
export PYTHONPATH=~/VXingest
outdir="/data/ctc_to_cb/rap_ops_130/output/${pid}"
nohup python ${clonedir}/ctc_to_cb/run_ingest_threads.py -s /data/ctc_to_cb/load_specs/load_spec_metar_ctc_rap_ops_130_V01.yaml  -c ~/adb-cb1-credentials -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs &
```

this will create or update a document with the id "MD:V01:METAR:HRRR_OPS:ingest:grib2"
and this document will contain all of the surface, ceiling, and visibility data defined in the template.

## Tests

There are unit tests in the test directory. To run the unit tests
switch to the VXingest directory and use this invocation.
This assumes that you have cloned this repo into your home directory,
and that you have installed any necessary python packages like numpy.

```bash
cd ~/VXingest
export PYTHONPATH=~/VXingest
python3 -m unittest ctc_to_cb/test/test_metar_model_grib.py
```

or

```bash
cd ~/VXingest
export PYTHONPATH=~/VXingest
python3 -m unittest ctc_to_cb/test/test_metar_grib_and_station_utils.py
```

## Examples of running the ingest programs

clonedir=~/VXingest
cd ~/VXingest
export PYTHONPATH=~/VXingest
outdir="/data/ctc_to_cb/rap_ops_130/output/${pid}"
python ${clonedir}/ctc_to_cb/run_ingest_threads.py -s /data/ctc_to_cb/load_specs/load_spec_metar_ctc_rap_ops_130_V01.yaml  -c ~/adb-cb1-credentials -o $outdir -t8

${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs &

### parameters

-s this is the load spec

-c this is the credentials file

-o output directory - this is the directory for the program to write output json files.

-t threads this is how many threads the program will use (optional, default is 1)

All of these examples assume that you have a proper python3 and that you have installed
python packages.

### N1QL metadata queries

These are mostly oriented around cb-ceiling but they are illustrative.
This will return the min and max fcstValidEpoch for the HRRR model

```bash
select min(mdata.fcstValidEpoch) as mindate, max(fcstValidEpoch) as maxdate from mdata where type="DD" and docType="model" and subset="METAR" and version="V01" and model='HRRR';
```

Alternatively the min and max fcstValidEpochs can be returned quickly with

```bash
select min(meta().id) as minid, max(meta().id) as maxid from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```

because the ids contain the fcstValidEpochs and the ids will sort by them.
This returns the nuber of METAR contingency tables for the HRRR
select count(meta().id) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
numrecs: 1248123,

This returns the current epoch
```select floor(NOW_MILLIS()/1000)```

This returns the distinct array of regions for the contingency tables for the HRRR

```bash
select raw array_agg(distinct mdata.region) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```

This returns the distinct array of forecast lengths for the contingency tables for the HRRR

```bash
select raw array_agg(distinct mdata.fcstLen) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```

This returns the distinct array of thresholds for the contingency tables for the HRRR. This takes a pretty long time,
around a minute.

```bash
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
**change user and password**

- This is the N1QL search mentioned above for returning the minimum fcstValidBeg
for all the METAR obs in the mdata bucket, executed with the curl rest api.

```bash
curl -s -u 'avid:getapassword' http://adb-cb4.gsd.esrl.noaa.gov:8093/query/service  -d 'statement=select min(mdata.fcstValidEpoch) as min_fcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type="DD" and docType = "obs" and subset = "METAR" and version is not missing' | jq -r '.results | .[] | .min_fcstValidEpoch'
```

This is the same but it returns the max fcstValidBeg

```bash
curl -s -u 'avid:getapassword' http://adb-cb4.gsd.esrl.noaa.gov:8093/query/service  -d 'statement=select min(mdata.fcstValidEpoch) as min_fcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type="DD" and docType = "obs" and subset = "METAR" and version is not missing' | jq -r '.results | .[] | .max_fcstValidEpoch'
```

- This returns a hit list with one hit for DIA.

```bash
curl -XPOST -H "Content-Type: application/json" -u 'avid:fakepassword' http://adb-cb4.gsd.esrl.noaa.gov:8094/api/index/station_geo/query -d '{"fields": ["*"],"query": {"fields":["*"], "regexp": "^denver.*","field":"description"}}' | jq '.'
```

This is a curl command that searches by lat and lon for stations within 1 mile of 39.86, -104.67 and it finds DIA

```bash
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

```bash
type="MD" and docType="ingest" and subset="METAR" and version="V01"
```

To retrieve all the ingest documents for METARS and restrict it to only CTC ingest documents.

```bash
type="MD" and docType="ingest" and subset="METAR" and version="V01" and subType="CTC"
```

To retrieve all the CTC documents for METARS and model HRRR

```bash
type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR"
```

To retrieve 10 CTC documents for HRRR METARS

```bash
select mdata.* from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR" limit 10
```

To retrieve 10 CTC documents for HRRR METARS at a specific fcstValidEpoch

```bash
select mdata.* from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR" and fcstValidEpoch=1516986000 limit 10
```

To count all the METAR model documents where the model is HRR_OPS
If you retrieve these documents and examine them you find that these documents have model
variables at specific fcstValidBeg and are organized by metar station.

```bash
select count(*) from mdata where type="DD" and docType="model" and subset="METAR" and version="V01" and model="HRRR_OPS"
```

To retrieve the metadata document for cb-ceiling for the HRRR model

```bash
select mdata.* from mdata where type="MD" and docType="mats_gui" and subType="app" and subset="COMMON" and version="V01" and app="cb-ceiling" and model="HRRR"
```

To retrieve the metadata document for cb-ceiling for all models

```bash
select mdata.* from mdata where type="MD" and docType="mats_gui" and subType="app" and subset="COMMON" and version="V01" and app="cb-ceiling"
```

## Initial configuration recommendations

For both the single server and the three node cluster it is most advisable to
run the Query, Index, and Data services on all the nodes.
With the single node server there are no replications possible, but for
the cluster we should start with num_recs = 2 (one less than the number of nodes) which
will result in three instances of each service.

## Example ingest commands

This is bounded by a time range -f 1437084000 -l 1437688800 data will not be retrieved from the sql tables outside this range.

```bash
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_GtLk_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials -f 1437084000 -l 1437688800
```

These are unbounded by a time range - all data that the ingest statement can retrieve will be retrieved.

```bash
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_ALL_HRRR_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_E_HRRR_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_E_US_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_GtLk_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_ALL_HRRR_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_E_HRRR_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_E_US_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_GtLk_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-hrrr_ops-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_W_HRRR_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_W_HRRR_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-metars-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-rrfs_dev1-v01.yaml -c ${HOME}/adb-cb1-credentials
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-stations-v01.yaml -c ${HOME}/adb-cb1-credentials
```

This script will consider all the above load_spec files and it will find the latest time for each that exists in the couchbase bucket
and the latest time that exists in the sql table and use those values as a total time range.
The total time range will be further divided into one week intervalse that will
be used to bound the run_gsd_ingest_threads.py. This script uses nohup because it might take a long time to run.
It also runs in the background and this example puts the output into a log file.

```bash
nohup ../scripts/VXingest_utilities/ingest.sh ~/adb-cb1-credentials > logs/ingest-20210326-10-15 2>&1 &

```
