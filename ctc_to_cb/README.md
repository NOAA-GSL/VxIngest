# grib2 ingest to couchbase

## purpose

These programs are intended to import grib data into Couchbase taking advantage of the GSL Couchbase data schema
that has been developed by the GSL AVID model verification team.

## Environment

In the test directory README ctc_to_cb/test/README.md you will find instructions for setting up the environment and for running the tests.
You can see a list of installed python packages that is known to satisfy requirements
in the python_packages.md file in this directory.

## Approach

These programs use a load_spec YAML file and a credentials file.

### load_spec example

This is the grib2_to_cb/test/load_spec_grib_metar_hrrr_ops_V01.yaml file from this distribution.

```json
email: "randy.pierce@noaa.gov"
  ingest_document_id: "MD:V01:METAR:HRRR_OPS:ingest:grib2"
  cb_connection:
    management_system: "cb"
    host: "cb_host"
    user: "cb_user"
    password: "cb_pwd"
```

The email is optional - currently not used.
The cb_connection block defines the connection values that will be used to authenticate a connection to the host.
The ingest_document_ids: ['MD:V01:METAR:HRRR_OPS:ingest:grib2'] line defines
one or a list of metadata documents. These documents define how the program will operate.
The 'MD:V01:METAR:obs:ingest:netcdf' value is the id of a couchbase metadata document.
This document MUST exist on the couchbase cluster defined by cb_host
and MUST be readable by the cb_user. Copies of the metadata documents are checked into
.../VXingest/mats_metadata_and_indexes/ingest_models_from_grib.json and there is a script
.../VXingest/mats_metadata_and_indexes/ingest_models_from_grib.sh that is usefull for importing
the associated metadata document.

### metadata Example

This is the contents of "MD:V01:METAR:HRRR_OPS:ingest:grib2". You must either
be certain that it already exists in the couchbase cluster or you must create it.

```json
    {
  "builder_type": "GribModelBuilderV01",
  "validTimeInterval": 3600,
  "validTimeDelta": 1800,
  "docType": "ingest",
  "subDocType": "grib2",
  "id": "MD:V01:METAR:HRRR_OPS:ingest:grib2",
  "model": "HRRR_OPS",
  "fcstLens": "0,1,3,6,9,12,15,18,21,24,27,30,33,36",
  "subType": "model",
  "subset": "METAR",
  "template": {
    "correctedTime": "",
    "data": {
      "&getName": {
        "Ceiling": "&handle_ceiling",
        "DewPoint": "&kelvin_to_farenheight|*2 metre dewpoint temperature",
        "Surface Pressure": "&handle_surface_pressure|*Surface pressure",
        "Temperature": "&kelvin_to_farenheight|*2 metre temperature",
        "Visibility": "&handle_visibility|*Visibility",
        "RH": "&handle_RH|*2 metre relative humidity",
        "WD": "&handle_wind_direction",
        "WS": "&handle_wind_speed|*10 metre U wind component,*10 metre V wind component",
        "name": "&getName"
      }
    },
    "dataSourceId": "NCO",
    "docType": "model",
    "model": "HRRR_OPS",
    "fcstValidBeg": "&handle_iso_time",
    "fcstValidEpoch": "&handle_time",
    "id": "DD:V01:METAR:HRRR_OPS:&handle_time:&handle_fcst_len",
    "subset": "METAR",
    "type": "DD",
    "version": "V01"
  },
  "type": "MD",
  "version": "V01"
}
 
```

The line
```"builder_type": "GribModelBuilderV01"```
defines a python class. These builder classes are defined
in the grib_builder.py file. This class will interpret the
load_spec and ingest data from a set of grib2 files retrieved from the path.
Notice

```code
    "type": "MD",
    "version": "V01",
    "docType": "ingest",
    "subset": "METAR",
```

#### field substitution by value in the template

These fields describe a metadata document that is used by a program to ingest data.
Data documents will be created according to the template defined in the "template" field.
Template strings that start with an '*' will be replaced with data derived
from the grib file. Most template entries in the GribBuilders are handler functions. A handler function
is defined like ```"&handle_surface_pressure|*Surface pressure"```. In this example the function name is
handle_surface_pressure and it will recieve a parameter param_dict['Surface pressure'] that will be an array of
tuples, one per station location. Each tuple will have a location precise value and an interpolated value.

#### field substitution by function in the template

If in the template above the line ```"Surface Pressure": "&handle_surface_pressure|*Surface pressure",```
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

#### Where to place substitutions

Substitutions can be for keys or values in the template, in top level documents or in sub documents.

## Structure of templates

Templates are given document identifiers like
```MD:V01:METAR:HRRR_OPS:ingest:grib2```
This identifier is constrained to match specific fields within the
document. "type:version:subset:product:docType

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
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getapassword' --format list --generate-key %id% --dataset file:///${HOME}/VXingest/gsd_sql_to_cb/ingest_backup/ingest-20210313:083606

```

#### Restore other metadata with cbimports

Refer to the VXingest/gsd_sql_to_cb/metadata_files/regions.json for
an example of a multi-document metadata file. The cbimport command for importing these region definitions
into a server would look like this for the server adb-cb4.gsd.esrl.noaa.gov
The password has been obscured and the example assumes that you cloned this repo into ${HOME}....

```code
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getyourselfapassword' --format list --generate-key %id% --dataset file:///${HOME}/VXingest/gsd_sql_to_cb/metadata_files/regions.json
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
nohup python grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/hrrr/conus/wrfprs/grib2/ -m %y%j%H%f -o /data/grib2_to_cb/output -t 16 > logs/20210724-13:36 2>&1 &
```

this will create or update a document with the id "MD:V01:METAR:HRRR_OPS:ingest:grib2"
and this document will contain all of the surface, ceiling, and visibility data defined in the template.

## Tests

There are unit tests in the test directory. To run the unit tests
switch to the VXingest directory and use this invocation.
This assumes that you have cloned this repo into your home directory,
and that you have installed any necessary python packages like pygrib and numpy.

```bash
cd ~/VXingest
export PYTHONPATH=~/VXingest
python3 -m unittest grib2_to_cb/test/test_metar_model_grib.py
```

or

```bash
cd ~/VXingest
export PYTHONPATH=~/VXingest
python3 -m unittest grib2_to_cb/test/test_metar_grib_and_station_utils.py 
```

## Examples of running the ingest programs

cd ~/VXingest
export PYTHONPATH=~/VXingest
nohup  python grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/hrrr/conus/wrfprs/grib2/ -m %y%j%H%f -o /data/grib2_to_cb/output -t 8 > logs/20210724-09:12 2>&1 &

### parameters

-s this is the load spec

-c this is the credentials file

-p this is the path to the model output grib2 files

-m %y%j%H%f this is a python datetime.strptime format string that is used as a file name mask for the input files
>(NOTE: the %f equates to microseconds and essentially ignores the last 6 digits. For these model files
the last 6 digits refer to non date fields used for cycle init hour and minute, and forecast length)

-o output directory - this is the directory for the program to write output json files.

-n number of stations this is how many stations to process (optional, default is all of them)
>(NOTE: this is primarily useful only for debugging. It limits the processing)

-f this is first fcst valid epoch allowed to get ingested (optional, defau;lt is 0)

-l this is the last fcstValid epoch to get ingested (optional, default is max)

-t threads this is how many threads the program will use (optional, default is 1)

All of these examples assume that you are in the VxIngest/grib2_to_cb
directory. It also assumes that you have a proper python3 and that you have installed
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
