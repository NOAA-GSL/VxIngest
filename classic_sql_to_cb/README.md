# GSL database ingest to couchbase
## purpose
These programs are intended to import GSL 
database tables into Couchbase taking advantage of the GSL Couchbase data schema
that has been developed by the GSL AVID model verification team.
##Environment
These programs require python3, and couchbase sdk 3.0 (see [couchbase sdk](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html) )

You can see a list of installed python packages that is known to satisfy requirements
in the python_packages.md file in this directory.

##Approach
These programs use a load_spec YAML file and a credentials file.
### load_spec example
This is the test/load_spec_stations.yaml file from this distribution.
```
load_spec:
  email: "randy.pierce@noaa.gov"
  cb_connection:
    management_system: cb
    host: "cb_host"
    user: avid
    password: gsl_pwd
  mysql_connection:
    management_system: mysql
    host: "mysql_host"
    user: readonly
    password: readonly_pwd
  ingest_document_ids: ['MD:V01:METAR:stations_ingest']
```
The email is optional - currently not used.
The cb_connection block defines the connection values that will be used 
to authenticate a connection to the host.
The mysql_connection defines the connection to a mysql database.
The ingest_document_ids: ['MD:V01:METAR:stations_ingest'] line defines
a list of metadata documents. These documents define how thr program will operate.
The 'MD::V01::METAR::stations_ingest' value is the id of a couchbase metadata document.
This document MUST exist on the couchbase cluster defined by cb_host
and MUST be readable by the cb_user.
### metadata Example
This is the contents of "MD:V01:METAR:stations_ingest". If
you intend to use a metadata ingest document you must either
be certain that it already exists or you must create it. They
are all pretty much like this.
```
{
  "type": "MD",
  "docType": "ingest",
  "subset": "METAR",
  "version": "V01",
  "builder_type": "SqlStationsBuilderV01",
  "statement": "select UNIX_TIMESTAMP() as updateTime, m.name, m.madis_id, m.lat, m.lon, m.elev, s.disc as description, s.first, s.last, l.last_time from madis3.metars_mats_global as m, madis3.stations as s, madis3.locations as l where 1=1 and m.name = s.name and m.lat = l.lat and m.lon = l.lon and m.elev = l.elev;",
  "template": {
    "id": "MD:V01:METAR:stations",
    "type": "MD",
    "docType": "stations",
    "subset": "METAR",
    "dataFileId": "DF_id",
    "dataSourceId": "DS_id",
    "version": "V01",
    "updateTime": "*updateTime",
    "data": {
      "*name": {
        "name": "*name",
        "lat": "*lat",
        "lon": "*lon",
        "elev": "*elev",
        "description": "*description",
        "firstTime": "*first",
        "lastTime": "*last"
      }
    }
  }
}
```
The line
```"builder_type": "SqlStationsBuilderV01"```
defines a python class. These builder classes are defined 
in the gsd_builder.py file. This class will interpret the
load_spec and ingest data that is returned by the mysql statement
in the "statement" field. Whether the entire result set is combined
into one document or multiple documents depends on the "builder_type".
In this example the "SqlStationsBuilderV01" combines all 
the data into one document with the data fields ingested as top level
entries.
Notice 
```
    "type": "MD",
    "version": "V01",
    "docType": "ingest",
    "subset": "METAR",
```
####field substitution by value in the template
These fields describe a metadata document that is used by a program to ingest data.
Data documents will be created according to the template defined in the "template" field.
Template strings that start with an '*' will be replaced with data returned
from the sql query. For example the key "\*name" might be replaced
with "KDEN"  returned on one row of the result set in the "name"
field. In like manner 
the value "*description" will be replaced with the actual description text that
was returned in the description field of the row. This example does not illustrate combinations of 
replacement fields, but you could have a replacement field like "\*field1\*field2"
which would result in the values represented by field1 and field2 being
concatenated together in the result.

The ingest document "MD:V01:METAR:stations:ingest "
```
{
  "type": "MD",
  "docType": "ingest",
  "subset": "METAR",
  "version": "V01",
  "builder_type": "SqlStationsBuilderV01",
  "singularData": true,
  "statement": "select UNIX_TIMESTAMP() as updateTime, m.name, m.madis_id, m.lat, m.lon, m.elev, s.disc as description, s.first, s.last, l.last_time from madis3.metars_mats_global as m, madis3.stations as s, madis3.locations as l where 1=1 and m.name = s.name and m.lat = l.lat and m.lon = l.lon and m.elev = l.elev;",
  "template": {
    "id": "DD:V01:METAR:station:*name",
    "type": "DD",
    "docType": "station",
    "subset": "METAR",
    "dataFileId": "DF_id",
    "dataSourceId": "DS_id",
    "version": "V01",
    "updateTime": "*updateTime",
    "description": "*description",
    "firstTime": "*first",
    "lastTime": "*last",
    "station": "*name",
    "name": "*name",
    "notlat": "*lat",
    "notlon": "*lon",
    "geo": {
      "lat": "*lat",
      "lon": "*lon",
      "elev": "*elev"
    }
  }
}
```

The ingest document defines a builder type SqlStationsBuilderV01 which will create 
a metadata document that has all of the stations contained in a data list.

####field substitution by function in the template
If in this template the line ```"lat": "*lat"``` were replaced with 
```"&conv_latlon:*lat"``` it would define a field substitution by defined function. A defined function must 
exist in the specified builder class. These functions have a signature like 
```    
@staticmethod
def conv_latlon(meta_data, params_dict):
...
 ```
The template line is divided into two parts that are separated by
a ":". The first part specifies a function name ```conv_latlon``` and
the second part specifies a parameter list that will be converted
into a dict structure and passed into the named function.
The parameter dict will have a key, in this case the parameter will be
something like {'lat': latitude} where latitude will be the real
lat value from the current data set.

#### Where to place substitutions
Substitutions can be for keys or values in the template, in top level documents or in sub documents.
## Structure of templates
Templates are given document identifiers like
```MD:V01:METAR:stations:ingest```
This identifier is constrained to match specific fields within the 
document. "type:version:subset:product:docType

and MUST contain these keywords...
 ```
  "type": "MD",  - required to be 'MD'
  "docType": "ingest",  - required to be 'ingest'
  "subset": "METAR",  - required set to whatever is appropriate
  "version": "V01",  - the version of the template
  "product": "stations"
  "builder_type": "some builder class",
  "singularData": true,   - true if only one document is to be produced
  "statement": "some statemnet",
```
## Backup ingest documents!!!
Ingest documents can be backed up with a utility in the scripts/VX_ingest_utilities
directory... save_ingest_docs_to_csv.sh
This utility requires a backup directory which is nominally
VXingest/gsd_sql_to_cb/ingest_backup, and a server name. The utility will backup all the currently defined ingest documents
based on the id pattern "MD.*:ingest".

##### Example
```
${HOME}/VXingest/scripts/VXingest_utilities/save_ingest_docs_to_csv.sh ${HOME}/VXingest/gsd_sql_to_cb/ingest_backup adb-cb1.gsd.esrl.noaa.gov
```
##### Alternatively for personal backups:
you can use the document export and import utility on the couchbase UI IF THE COUCHBASE
SERVER VERSION IS GREATER THAN 6.5.

To use the UI navigate to the UI Query page

https://adb-cb4.gsd.esrl.noaa.gov:18091/ui/index.html#!/query

Enter this query into the query editor and execute the query.
```
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
```
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getapassword' --format list --generate-key %id% --dataset file:///${HOME}/VXingest/gsd_sql_to_cb/ingest_backup/ingest-20210313:083606
```
#### Restore other metadata with cbimports
Refer to the VXingest/gsd_sql_to_cb/metadata_files/regions.json for
an example of a multi-document metadata file. The cbimport command for importing these region definitions
into a server would look like this for the server adb-cb4.gsd.esrl.noaa.gov
The password has been obscured and the example assumes that you cloned this repo into ${HOME}....
```
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getyourselfapassword' --format list --generate-key %id% --dataset file:///${HOME}/VXingest/gsd_sql_to_cb/metadata_files/regions.json
```
For more information on cbimport see [cbimport](https://docs.couchbase.com/server/current/tools/cbimport-json.html)
## Credentials files
This is an example credentials file, the user and password are fake.
```
  cb_host: adb-cb1.gsd.esrl.noaa.gov
  cb_user: a_gsd_user
  cb_password: A_gsd_user_password
  mysql_host: wolphin.fsl.noaa.gov
  mysql_user: a_gsd_mysql_user
  mysql_password: a_gsd_mysql_user_password

```
You must have both of these files and you must supply them to 
the program as a parameter.
##Example invocation
This assumes that you have cloned this repo into your home directory.
```
export PYTHONPATH=~/VxIngest
~/anaconda3/bin/python3 gsd_sql_to_cb/run_gsd_ingest_threads.py -s /home/pierce/VxIngest/test/load_spec_gsd-stations_v03.yaml -c /home/pierce/adb-cb4-credentials
```
this will create or update a document with the id "MD:V01:METAR:stations"
and this document will contain all of the stations that are in the 
GSD madis3.stations table. 

##Tests
There are tests in the test directory. To run the connections test
for example switch to the test directory and use this invocation.
This assumes that you have cloned this repo into your home directory.

```
export PYTHONPATH=~/VxIngest
~/anaconda3/bin/python3 -m unittest test_connection.py```
```
##Examples of running the ingest programs
#### parameters
-s this is the load spec

-c this is the credentials file

-f this is the first fcstValid epoch to get ingested 

-l this is the last fcstValid epoch to get ingested

All of these examples assume that you are in the VxIngest/gsd_sql_to_cb
directory. It also assumes that you have a proper python3 and that you have installed 
python packages.
###Ingest version 3 stations
This needs no first and last epoch parameter

```
export PYTHONPATH=$HOME/VXingest
$HOME/VXingest/gsd_sql_to_cb/run_gsd_ingest_threads.py
-s $HOME/VxIngest/test/load_spec_gsd-stations-v03.yaml
-c $HOME/adb-credentials-local
```
###Ingest version 4 METAR obs
This will ingest all records from Thursday, November 12, 2020 12:00:00 AM
(epoch 1605139200) through Saturday, November 14, 2020 12:00:00 AM 
(epoch 1605312000) INCLUSIVE.
```
export PYTHONPATH=$HOME/VXingest
$HOME/VXingest/gsd_sql_to_cb/run_gsd_ingest_threads.py
-s $HOME/VxIngest/test/load_spec_gsd-metars-v04.yaml
-c $HOME/adb-credentials-local
-f 1605139200
-l 1605312000
```
## Recomended indexes
There are a lot of administrative scripts for dealing with indexes in the 
...scripts/admin/index directory. Full-text-search index utilities are in the 
...scripts/admin/fts directory.
This utility can be used to retrieve all the currently defined indexes from a cluster.
This invocation filters for the mdata bucket.
.../VXingest/scripts/admin/index/index-definitions.sh --cluster=adb-cb4.gsd.esrl.noaa.gov --username=avid --password='pwd' | grep mdata
(the password is not actual)

The create...n1ql files in the ...mats_metadata_and_indexes/index_creation_scripts folder were created with this utility. 

There are index creation n1ql scripts in this directory. The scripts labeled ...-0-relicas.n1ql
will not produce replicas, i.e. use them on a standalone server, and the scripts labeled
...-2-replicas.n1ql should be used for the cluster.

They can be loaded with the ....scripts/admin/index/index-import-all-indexes.sh
script.
e.g.
.../VXingest/scripts/admin/index/index-import-all-indexes.sh --cluster=adb-cb4.gsd.esrl.noaa.gov --username=avid --password='pwd' --file=.../mats_metadata_and_indexes/index_creation_scripts/create_indexes-2-replicas.n1ql
(the password is not actual)

Alternatively you can load the indexes in the query console of the UI and then 
use the command
```
BUILD INDEX ON mdata (( SELECT RAW name FROM system:indexes WHERE keyspace_id = "mdata" AND state = 'deferred' ));
```
to actually build the indexes.

##Useful and interesting queries
- This [page](https://docs.couchbase.com/server/current/fts/fts-geospatial-queries.html) talks about geospatial queries.
- This [page](https://docs.couchbase.com/server/current/fts/fts-searching-from-the-ui.html) talks about full text searches from the UI. Note that UI FTS searches are pretty limited.
- This [page](https://docs.couchbase.com/server/current/fts/fts-searching-with-the-rest-api.html)] talks about using curl.
### Important note about OUR N1QL queries and indexes.
Each N1QL query requires an index to work. Our basic indexes
cover the type, docType, version, and subset fields.
That means, for our case, that you must have
```
WHERE type="DD"
and docType = "station"
and subset = "METAR"
and version = "V01"
```
in each of your N1QL queries.
 
### N1QL queries
This query returns all the metadata id's and updated time.
```
select updated, meta().id from mdata where type="MD" and docType is not missing and version = "V01"  and subset="COMMON"
```
This query modifies the above to only return for the models "HRRR" and "HRRR_OPS".
```
select meta().id, updated from mdata where type="MD" and docType is not missing and version = "V01"  and subset="COMMON" and model in ["HRRR", "HRRR_OPS"];
```

This query returns the minimum fcstValidBeg and the maximum
fcstValidBeg for all the METAR obs in the mdata bucket.
```
select min(mdata.fcstValidBeg) as min_fcstValidBeg, max(mdata.fcstValidBeg) as max_fcstValidBeg
from mdata
WHERE type="DD"
and docType = "obs"
and subset = "METAR"
and version is not missing 
```
This is the same thing but with epochs, which is useful for setting
parameters for ingest.
```
select min(mdata.fcstValidEpoch) as min_fcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch
from mdata
WHERE type="DD"
and docType = "obs"
and subset = "METAR"
and version is not missing
```

This query will return a lot of results without further filtering in the predicates.
``` 
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
```
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
```
select raw station
from mdata as station
WHERE type="DD"
and docType = "station"
and subset = "METAR"
and version = "V01"
and SEARCH(station.name,"KDEN")
```
This is another way to do the above query.
```
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
```
select raw station
from mdata as station
WHERE type="DD"
and docType = "station"
and subset = "METAR"
and version = "V01"
and SEARCH(station,{"field":"description", "regexp": "denver.*"})
```
### N1QL metadata queries
These are modtly oriented around cb-ceiling but they are illustrative.
This will return the min and max fcstValidEpoch for the HRRR model
```
select min(mdata.fcstValidEpoch) as mindate, max(fcstValidEpoch) as maxdate from mdata where type="DD" and docType="model" and subset="METAR" and version="V01" and model='HRRR';
```
Alternatively the min and max fcstValidEpochs can be returned quickly with 
```
select min(meta().id) as minid, max(meta().id) as maxid from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```
because the ids contain the fcstValidEpochs and the ids will sort by them.
This returns the nuber of METAR contingency tables for the HRRR 
select count(meta().id) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
numrecs: 1248123,

This returns the current epoch
```select floor(NOW_MILLIS()/1000)```

This returns the distinct array of regions for the contingency tables for the HRRR
```
select raw array_agg(distinct mdata.region) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```
This returns the distinct array of forecast lengths for the contingency tables for the HRRR
```
select raw array_agg(distinct mdata.fcstLen) from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model='HRRR';
```
This returns the distinct array of thresholds for the contingency tables for the HRRR. This takes a pretty long time,
around a minute.
```
SELECT DISTINCT RAW d_thresholds
FROM (SELECT OBJECT_NAMES (mdata.data) AS thresholds
      FROM  mdata 
      WHERE  type="DD" AND docType="CTC" AND  subset="METAR" AND  version="V01" AND model='HRRR')  AS d
UNNEST d.thresholds AS d_thresholds;
```

##Useful curl queries
Curl queries can be implemented on the command line or in the client SDK.
This is an example of doing a regular expression query for the word "denver" (case insensitive because of the search index analyzer) at the front of any description. The results are piped into jq to make them pretty. 
The password is fake so replace it with the actual password.

**change user and password**
- This is the N1QL search mentioned above for returning the minimum fcstValidBeg
for all the METAR obs in the mdata bucket, executed with the curl rest api.

```
curl -s -u 'avid:getapassword' http://adb-cb4.gsd.esrl.noaa.gov:8093/query/service  -d 'statement=select min(mdata.fcstValidEpoch) as min_fcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type="DD" and docType = "obs" and subset = "METAR" and version is not missing' | jq -r '.results | .[] | .min_fcstValidEpoch'
```

This is the same but it returns the max fcstValidBeg
```
curl -s -u 'avid:getapassword' http://adb-cb4.gsd.esrl.noaa.gov:8093/query/service  -d 'statement=select min(mdata.fcstValidEpoch) as min_fcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type="DD" and docType = "obs" and subset = "METAR" and version is not missing' | jq -r '.results | .[] | .max_fcstValidEpoch'
```

- This returns a hit list with one hit for DIA.

```
curl -XPOST -H "Content-Type: application/json" -u 'avid:fakepassword' http://adb-cb4.gsd.esrl.noaa.gov:8094/api/index/station_geo/query -d '{"fields": ["*"],"query": {"fields":["*"], "regexp": "^denver.*","field":"description"}}' | jq '.'
```

This is a curl command that searches by lat and lon for stations within 1 mile of 39.86, -104.67 and it finds DIA 

```
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
```
type="MD" and docType="ingest" and subset="METAR" and version="V01"
```
To retrieve all the ingest documents for METARS and restrict it to only CTC ingest documents.
```
type="MD" and docType="ingest" and subset="METAR" and version="V01" and subType="CTC"
```
To retrieve all the CTC documents for METARS and model HRRR
```
type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR"
```
To retrieve 10 CTC documents for HRRR METARS
```
select mdata.* from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR" limit 10
``` 

To retrieve 10 CTC documents for HRRR METARS at a specific fcstValidEpoch
```
select mdata.* from mdata where type="DD" and docType="CTC" and subset="METAR" and version="V01" and model="HRRR" and fcstValidEpoch=1516986000 limit 10
```
To count all the METAR model documents where the model is HRR_OPS
If you retrieve these documents and examine them you find that these documents have model 
variables at specific fcstValidBeg and are organized by metar station.
```
select count(*) from mdata where type="DD" and docType="model" and subset="METAR" and version="V01" and model="HRRR_OPS"
```
To retrieve the metadata document for cb-ceiling for the HRRR model
```
select mdata.* from mdata where type="MD" and docType="mats_gui" and subType="app" and subset="COMMON" and version="V01" and app="cb-ceiling" and model="HRRR"
``` 
To retrieve the metadata document for cb-ceiling for all models
```
select mdata.* from mdata where type="MD" and docType="mats_gui" and subType="app" and subset="COMMON" and version="V01" and app="cb-ceiling"
``` 

## Initial configuration recommendations
For both the single server and the three node cluster it is most advisable to 
run the Query, Index, and Data services on all the nodes.
With the single node server there are no replications possible, but for
the cluster we should start with num_recs = 2 (one less than the number of nodes) which
will result in three instances of each service.

##Example ingest commands
This is bounded by a time range -f 1437084000 -l 1437688800 data will not be retrieved from the sql tables outside this range.
```
python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-HRRR_GtLk_CTC-v01.yaml -c ${HOME}/adb-cb1-credentials -f 1437084000 -l 1437688800
```
These are unbounded by a time range - all data that the ingest statement can retrieve will be retrieved.
```
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
```
nohup ../scripts/VXingest_utilities/ingest.sh ~/adb-cb1-credentials > logs/ingest-20210326-10-15 2>&1 &

```

