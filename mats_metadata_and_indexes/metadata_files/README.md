# MATS metadata and ingest documents

The system relies on a couple of kinds of metadata. Ingest builders use ingest documents to control how an output document is created from its input source. The running system relies on various metadata documents to define what data is available and how it should be represented in a GUI interface.

## Ingest documents

Ingest documents contain templates that define how a builder should assemble an output document.

### ingest document Example

NOTE: prepbufr ingest documents have an extra element called "mnemonic_mapping" that maps prepbufr mnemonics to the template variables. This extra step is necessary because prepbufr fields can vary due to an associated program code.
For example a program code of 1 is an initial value and a program code of 8 is a virtual value. The template.mnemonic_mapping section specifies whether the event program code is meaningful and which program code value is desired. Refer to the prepbufr [README.md](https://github.com/NOAA-GSL/VxIngest/blob/72793df75696ef711d79553a82be3b8a6c04653c/src/vxingest/prepbufr_to_cb/README.md) for a complete example and explanation of the prepbufr ingest template.

This is the contents of "MD:V01:METAR:obs:ingest:netcdf". If
you intend to use a metadata ingest document you must either
be certain that it already exists or you must create it.

```json
{
  "builder_type": "NetcdfMetarObsBuilderV01",
  "validTimeInterval": 3600,
  "validTimeDelta": 1800,
  "docType": "ingest",
  "subDocType": "netcdf",
  "id": "MD:V01:METAR:obs:ingest:netcdf",
  "requires_time_interpolation": true,
  "subType": "obs",
  "subset": "METAR",
  "template": {
    "correctedTime": "",
    "data": {
      "*stationName": {
        "Ceiling": "&ceiling_transform|*skyCover,*skyLayerBase",
        "DewPoint": "&kelvin_to_farenheight|*dewpoint",
        "Reported Time": "&umask_value_transform|*timeObs",
        "Surface Pressure": "&handle_pressure|*altimeter",
        "Temperature": "&kelvin_to_farenheight|*temperature",
        "Visibility": "&handle_visibility|*visibility",
        "WD": "&umask_value_transform|*windDir",
        "WS": "&meterspersecond_to_milesperhour|*windSpeed",
        "name": "&handle_station|*stationName"
      }
    },
    "units": {
      "Ceiling": "ft",
      "DewPoint": "deg F",
      "Surface Pressure": "mb",
      "Temperature": "deg F",
      "Visibility": "miles",
      "RH": "percent",
      "WD": "degrees",
      "WS": "mph"
    },
    "dataSourceId": "MADIS",
    "docType": "obs",
    "fcstValidISO": "&derive_valid_time_iso|%Y%m%d_%H%M",
    "fcstValidEpoch": "&derive_valid_time_epoch|%Y%m%d_%H%M",
    "id": "DD:V01:METAR:obs:&derive_valid_time_epoch|%Y%m%d_%H%M",
    "subset": "METAR",
    "type": "DD",
    "version": "V01"
  },
  "type": "MD",
  "version": "V01"
}
```

### Ingest Template DSL

Ingest templates implement a simple DSL (Domain specific Language) described here for a netcdf builder. The same syntax applies to all builder classes.

The line
```"builder_type": "NetcdfObsBuilderV01"```
defines a python class. These builder classes are defined
in the [netcdf_to_cb/netcdf_builder.py](https://github.com/NOAA-GSL/VxIngest/blob/main/netcdf_to_cb/netcdf_builder.py) file. This class will interpret the
ingest data from a set of netcdf files retrieved from the path.
Whether the entire result set is combined into one document or multiple documents depends on the "builder_type".
In this example the "NetcdfObsBuilderV01" combines all
the data into one document with the data fields ingested as top level
entries.
Notice

```code
    "type": "MD",
    "version": "V01",
    "docType": "ingest",
    "subset": "METAR",
```

These fields must be present in any ingest document.

### field substitutions

Field substitutions are how templates associate data from the builder input with its position in the output document.

#### field substitution by value in the template

These fields describe a metadata document that is used by a program to ingest data.
Data documents will be created according to the template defined in the "template" field.
Template strings that start with an '*' will be replaced with data returned
from the netcdf file. For example the key "\*name" might be replaced
with "KDEN"  returned on one row of the result set in the "name"
field. In like manner the value "*description" will be replaced with the actual description text that
was returned in the file. This example does not illustrate combinations of
replacement fields, but you could have a replacement field like "\*field1\*field2"
which would result in the values represented by field1 and field2 being
concatenated together in the result.

#### field substitution by function in the template

If in the template above the line ```"Ceiling": "&ceiling_transform|*skyCover,*skyLayerBase"```
defines a field substitution by named function. A named function must
exist in the specified builder class, or its parent class. These functions have a signature like

```code
def ceiling_transform(self, params_dict):
...
 ```

The named function routine processes a named function entry from a template.
The '_named_function_def' looks like "&named_function|*field1,*field2,*field3..."
where "named_function" is the literal function name of a defined function.
The name of the function and the function parameters are separated by a "|" and
the parameters are separated by a ','.
It is expected that field1, field2, and field3 etc are all valid variable names or constants.
Each non constant field will be translated from the netcdf file into value1, value2 etc. Each
constant will be positioned as is in the output document.
The method "named_function" will be called like...
```named_function({field1:value1, field2:value2, ... fieldn:valuen})``` and the return value from named_function
will be substituted into the generated document.

#### Where to place field substitutions

Substitutions can be for keys or values in the template, in top level documents or in sub documents.

### Structure of templates

Templates are given document identifiers like
```MD:V01:METAR:stations:ingest```
This identifier is constrained to match specific fields within the
document. "type:version:subset:product:docType

and MUST contain these keywords...

 ```code
  "type": "MD",  - required to be 'MD'
  "docType": "ingest",  - required to be 'ingest'
  "subset": "METAR",  - required set to whatever is appropriate
  "version": "V01",  - the version of the template
  "product": "stations"
  "builder_type": "some builder class"
```

### ingest document source location

It is reccomended that all ingest documents be kept in source code in the [mats_metadata_and_indexes/metadata_files](https://github.com/NOAA-GSL/VxIngest/tree/main/mats_metadata_and_indexes/metadata_files) directory. There is an associated import.sh script for each ingest set of ingest documents. The ingest documents are associated in json files that contain a list of associated ingest documents. If you modify ingest documents, modify them in source code and deploy them to the servers with the scripts.

### personal backup of ingest documents

Ingest documents can be backed up with a utility in the scripts/VX_ingest_utilities
directory... save_ingest_docs_to_csv.sh
This utility requires a backup directory which is nominally
VXingest/gsd_sql_to_cb/ingest_backup, and a server name. The utility will backup all the currently defined ingest documents based on the id pattern "MD.*:ingest".

### Example backup

``` sh
${HOME}/VXingest/scripts/VXingest_utilities/save_ingest_docs_to_csv.sh ${HOME}/VXingest/gsd_sql_to_cb/ingest_backup adb-cb1.gsd.esrl.noaa.gov
```

### Alternative for personal backups

you can use the document export and import utility on the couchbase UI IF THE COUCHBASE
SERVER VERSION IS GREATER THAN 6.5.

To use the UI navigate to the UI Query page

[adb-cb4-query](https://adb-cb4.gsd.esrl.noaa.gov:18091/ui/index.html#!/query

Enter this query into the query editor and execute the query.

```sql
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

### Restore ingest documents on local server

Ingest documents can be restored from the documents page IF YOU HAVE ADMINISTRATOR privileges
and the server VERSION IS 6.6 OR GREATER.
This is useful to restore ingest documents to your laptop.

Login wih administrator privileges. The go to the documents page.

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

``` sh
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getyourselfapassword' --format list --generate-key %id% --dataset file:///path_to_the_file
```

and to use multiple threads and log any errors you can do this, you should not use more threads than you have cpu cores....

``` sh
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getyourselfapassword' --format list --generate-key %id% --dataset file:///path_to_the_file -t num_threads -e an_error_file
```

A strategy might be to seperate data into multiple files and run a different cbimport
instance on each file.


## MATS metadata documents

These are documents that are used by NATS to define selectors with valid ranges of data and to specify display priorities and properties.

## Finding metadata in the couchbase UI

All of the metadata has a document type "MD". There, however, lots of metadata documents. To narrow down the list you can do a query like ...

``` sql
SELECT meta(mdata).id
FROM mdata
WHERE 
    type="MD"
    and docType != "ingest"
    AND docType != "station"
```

This will exclude the thousands of station documents and all the ingest documents.

### matsAux metadata

the matsAux metadata document "MD:matsAux:COMMON:V01" contains the standardizedModelList, the primaryModelOrders, and thresholdDescriptions. This metadata is created manually.
Go to the documents page and put  "type="MD" AND docType = "matsAux" AND subset='COMMON'  AND version='V01'"
into the N1QL WHERE box. The document is the auxilliary mats metadata document. Alternatively use the id in the id box and retrieve the document that way.

Or use cbq,
cbq> select * from mdata use keys "MD:matsAux:COMMON:V01";

### common metadata

There are common metadata documents. These will be type "MD" documents with a subset "COMMON" and no app field. You can find them with the following query...

``` sql
SELECT meta(mdata).id
FROM mdata
WHERE
    type="MD"
    AND subset = "COMMON"
    AND app is missing
```

### app specific metadata

Each model and app combination will require app specific metadata. For cb-ceiling metadata, for example, you could query like ....

``` sql

SELECT meta(mdata).id
FROM mdata
WHERE
    type="MD"
    AND app = "cb-ceiling"
```

## metadata document creation

Some of thge metadata is dynamically created by a script that queries the actual data and updates one or more metadata documents. See [update_ceiling_mats_metadata.sh](https://github.com/NOAA-GSL/VxIngest/blob/main/mats_metadata_and_indexes/metadata_files/update_ceiling_mats_metadata.sh)

The base templates for some of these metadata documents are controlled in github. In the vxingest project the directories are

``` json
[mats_metadata_and_indexes/metadata_files/auxilliary_mats_metadata.json](https://github.com/NOAA-GSL/VxIngest/blob/main/mats_metadata_and_indexes/metadata_files/auxilliary_mats_metadata.json)
and 
[mats_metadata_and_indexes/metadata_files/ceiling_mats_metadata.json](https://github.com/NOAA-GSL/VxIngest/blob/main/mats_metadata_and_indexes/metadata_files/ceiling_mats_metadata.json)
```

It is better to change any base template in the code, check it in and then use the
[import_ceiling_auxilliary.sh](https://github.com/NOAA-GSL/VxIngest/blob/main/mats_metadata_and_indexes/metadata_files/import_ceiling_auxilliary.sh)
script to actually do the import. That script will import the ceiling_mats_metadata.json and the auxilliary_mats_metadata.json document templates as they are in the code.

## updating the actual metadata

Those template documents don't have up-to-date values, because the values get updated by
the update_ceiling_mats_metadata.sh script from the same directory. The actual values depend on the currently ingested data so what is in the git repo is really just a template.

Running the script is pretty straightforward. cd to the root of the vxingest local repo (where you cloned the VXingest repo) then

``` bash
> ./mats_metadata_and_indexes/metadata_files/import_ceiling_auxilliary.sh credentials_file

```

where credentials_file is the actual credentials file (full path). The script imports the document that you checked in. Typically this script is run automatically when the ingest is triggered.

