# GSD database ingest to couchbase
## purpose
These programs are intended to import GSD 
(currently the organization is GSL but the data came from GSD) 
database tables into Couchbase taking advantage of the GSL Couchbase data schema
that has been developed by the GSL AVID model verification team.
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
    user: gsd
    password: gsd_pwd
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
  "builder_type": "GsdStationsBuilderV01",
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
```"builder_type": "GsdStationsBuilderV01"```
defines a python class. These builder classes are defined 
in the gsd_builder.py file. This class will interpret the
load_spec and ingest data that is returned by the mysql statement
in the "statement" field. Whether the entire result set is combined
into one document or multiple documents depends on the "builder_type".
In this example the "GsdStationsBuilderV01" combines all 
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

The ingest document "MD:V03:METAR:stations:ingest "
```
{
  "type": "MD",
  "docType": "ingest",
  "subset": "METAR",
  "version": "V03",
  "builder_type": "GsdStationsBuilderV03",
  "singularData": true,
  "statement": "select UNIX_TIMESTAMP() as updateTime, m.name, m.madis_id, m.lat, m.lon, m.elev, s.disc as description, s.first, s.last, l.last_time from madis3.metars_mats_global as m, madis3.stations as s, madis3.locations as l where 1=1 and m.name = s.name and m.lat = l.lat and m.lon = l.lon and m.elev = l.elev;",
  "template": {
    "id": "DD:V03:METAR:station:*name",
    "type": "DD",
    "docType": "station",
    "subset": "METAR",
    "dataFileId": "DF_id",
    "dataSourceId": "DS_id",
    "version": "V03",
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

The ingest document defines a builder type GsdStationsBuilderV03 which will create 
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
```MD:V03:METAR:stations:ingest```
This identifier is constrained to match specific fields within the 
document. "type:version:subset:product:docType

and MUST contain these keywords...
 ```
  "type": "MD",  - required to be 'MD'
  "docType": "ingest",  - required to be 'ingest'
  "subset": "METAR",  - required set to whatever is appropriate
  "version": "V03",  - the version of the template
  "product": "stations"
  "builder_type": "some builder class",
  "singularData": true,   - true if only one document is to be produced
  "statement": "some statemnet",
```
## Backup templates!!!
templates can be backed up with a utility in the scripts/VX_ingest_utilities
directory... save_ingest_docs_to_csv.sh
This utility requires a backup directory which is nominally
VXingest/gsd_sql_to_cb/ingest_backup. The utility will backup all the currently defined ingest documents
based on the id pattern "MD.*:ingest".

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
~/anaconda3/bin/python3 gsd_sql_to_cb/run_gsd_ingest_threads.py -s /home/pierce/VxIngest/test/load_spec_gsd-stations.yaml -c /home/pierce/adb-cb4-credentials
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

###Ingest version 1 stations
This needs no first and last epoch parameter
```
export PYTHONPATH=$HOME/VXingest
$HOME/VXingest/gsd_sql_to_cb/run_gsd_ingest_threads.py
-s $HOME/VxIngest/test/load_spec_gsd-stations-v01.yaml
-c $HOME/adb-credentials-local
```
###Ingest version 2 stations
This needs no first and last epoch parameter

```
export PYTHONPATH=$HOME/VXingest
$HOME/VXingest/gsd_sql_to_cb/run_gsd_ingest_threads.py
-s $HOME/VxIngest/test/load_spec_gsd-stations-v02.yaml
-c $HOME/adb-credentials-local
```
###Ingest version 1 METAR obs
This will ingest all records from Thursday, November 12, 2020 12:00:00 AM
(epoch 1605139200) through Saturday, November 14, 2020 12:00:00 AM 
(epoch 1605312000) INCLUSIVE.
```
$HOME/VXingest/gsd_sql_to_cb/run_gsd_ingest_threads.py
-s $HOME/VxIngest/test/load_spec_gsd-metars-v01.yaml
-c $HOME/adb-credentials-local
-f 1605139200
-l 1605312000
```

###Ingest version 2 METAR obs
This will ingest all records from Thursday, November 12, 2020 12:00:00 AM
through Saturday, November 14, 2020 12:00:00 AM INCLUSIVE
```
$HOME/VXingest/gsd_sql_to_cb/run_gsd_ingest_threads.py
-s $HOME/VxIngest/test/load_spec_gsd-metars-v02.yaml
-c $HOME/adb-credentials-local
-f 1605139200
-l 1605312000
```
## Recomended indexes
There are two index creation n1ql scripts in this directory.
They can be loaded with the scripts/admin/index/index-import-all-indexes.sh
script.

##Useful queries
- select raw meta().id from mdata where docType="obs" and subset="METAR" and type="DD" and version="V01"
- 

##Useful curl queries
**change user and password**

```curl -u 'gsd:gsd_pwd' -d "statement=select raw meta().id from mdata where docType='obs' and subset='METAR' and type='DD' and version='V01'" http://localhost:8093/query/service``` 
between 1605139200 and 1605312000

curl -u 'gsd:gsd_pwd_av!d' -d "statement=select raw meta().id from mdata where docType='ingest' and subset='METAR' and type='MD' and version IN ['V01','V02']" http://localhost:8093/query/service

## Useful utilities
There is a scripts directory, much of which came from Couchbase training.
This directory contains many useful scripts for administration, monitoring, and accessing Couchbase statistics.

## Initial configuration recommendations
For both the single server and the three node cluster it is most advisable to 
run the Query, Index, and Data services on all the nodes.
With the single node server there are no replications possible, but for
the cluster we should start with num_recs = 2 (one less than the number of nodes) which
will result in three instances of each service.