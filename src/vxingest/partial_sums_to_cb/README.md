# partial sums ingest to couchbase

## purpose

This builder is intended to import partial sums data into Couchbase taking advantage of the GSL Couchbase data schema that has been developed by the GSL AVID model verification team.
The partial_sums_builder.py program provides a PartialSumsBuilder class that will read existing model and observation data and, using the formula above, create PartialSums documents that can be imported into the database.

## How Partial Sums tables are derived

Refer to [a google doc](https://docs.google.com/document/d/1i2JJ_T_CsTRWxzlOWFWGWhGxyhEoVBjPeIJb1_CtWdo)

These are the sums that we need to generate for all models.

* sum( (model - obs)^2 )
* sum( N )
* sum( (model - obs) )
* sum( model )
* sum ( obs )
* sum( abs(model - obs) )

These are the surface variables that we currently have. They are all floating point numbers.

* DewPoint
* Surface Pressure
* Temperature
* RH
* WD
* WS

## Approach

These programs use a load_spec YAML file to define which ingest templates are to be used, a credentials file to provide database authentication, command line parameters for run time options, and the associated ingest template documents from the database that are specified in the load_spec.yaml.

### load_spec example

This is the  partial_sums_to_cb/test/load_spec_metar_sums_surface_V01.yaml file from this distribution.

```json
load_spec:
  email: "randy.pierce@noaa.gov"
  ingest_document_ids: ["MD-TEST:V01:METAR:HRRR_OPS:ALL_HRRR:SUMS:SURFACE:ingest"]
```

The email is optional - currently not used.
The ingest_document_ids: ['MD-TEST:V01:METAR:HRRR_OPS:ALL_HRRR:SUMS:SURFACE:ingest'] line defines
one or a list of metadata documents. These documents define how the program will operate.
The 'MD-TEST:V01:METAR:HRRR_OPS:ALL_HRRR:SUMS:SURFACE:ingest' value is the id of a couchbase metadata ingest document.
This document MUST exist on the couchbase cluster defined by cb_host
and MUST be readable by the cb_user. Copies of the metadata documents are checked into
.../VXingest/mats_metadata_and_indexes/ingest_sumss.json and there is a script
.../VXingest/mats_metadata_and_indexes/ingest_sumss.sh that is usefull for importing
the associated metadata documents.

## ingest documents

[obs ingest documents](https://github.com/NOAA-GSL/VxIngest/blob/0edaa03be13d75812e19ecf295e952b46d255b8f/mats_metadata_and_indexes/metadata_files/ingest_stations_and_obs_netcdf.json)

## Builder class

The builder is [PartialSumsSurfaceModelObsBuilderV01](partial_sums_builder.py)

There is a base PartialSumsBuilder which has the generic code for reading a pair of documents and generating model obs sums and then there is a specialized PartialSumsSurfaceModelObsBuilderV01 that inherits from the generic class. Specific information for these classes is available in the docstring for each class.


## ingest documents - metadata

Refer to [ingest documents and metadata](https://github.com/NOAA-GSL/VxIngest/blob/77b73babf031a19ba9623a7fed60de3583c9475b/mats_metadata_and_indexes/metadata_files/README.md#L11)


## Examples of running the ingest programs

### metadata Example

This is the contents of "MD:V01:METAR:HRRR_OPS:E_US:SUMS:SURFACE:ingest". You must either
be certain that it already exists in the couchbase cluster or you must create it.

```json
  {
    "id": "MD:V01:METAR:HRRR_OPS:E_US:SUMS:SURFACE:ingest",
    "builder_type": "PartialSumsSurfaceModelObsBuilderV01",
    "docType": "ingest",
    "type": "MD",
    "version": "V01",
    "subset": "METAR",
    "subType": "SUMS",
    "subDocType": "SURFACE",
    "model": "HRRR_OPS",
    "region": "E_US",
    "template": {
      "dataFileId": "DF_id",
      "dataSourceId": "DS_id",
      "id": "DD:V01:METAR:HRRR_OPS:E_US:SUMS:SURFACE:&handle_time:&handle_fcst_len",
      "type": "DD",
      "docType": "SUMS",
      "subDocType": "SURFACE",
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
```"builder_type": "PartialSumsSurfaceModelObsBuilderV01"```
refers to a python class. This builder class is defined
in the sums_builder.py file. This class will interpret the
load_spec and ingest data from a set of model and observation documents to create the sums documents.
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
def surface_transform(params_dict):
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
The entire id line will therefore be something like "id": "DD:V01:METAR:HRRR_OPS:E_US:SUMS:SURFACE:1636466400:0".

#### Where to place substitutions

Substitutions can be for keys or values in the template, in top level documents or in sub documents.

## Structure of templates

Templates are given document identifiers like
```"MD:V01:METAR:HRRR_OPS:E_US:SUMS:SURFACE:ingest"```
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
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getapassword' --format list --generate-key %id% --dataset file:///${HOME}/VXingest/sums_to_cb/ingest_backup/ingest-20210313:083606

```

#### Restore other metadata with cbimports

Refer to the VXingest/gsd_sql_to_cb/metadata_files/regions.json for
an example of a multi-document metadata file. The cbimport command for importing these region definitions
into a server would look like this for the server adb-cb4.gsd.esrl.noaa.gov
The password has been obscured and the example assumes that you cloned this repo into ${HOME}....

```code
cbimport json --cluster couchbase://adb-cb4.gsd.esrl.noaa.gov --bucket mdata --username avid --password 'getyourselfapassword' --format list --generate-key %id% --dataset file:///${HOME}/VXingest/sums_to_cb/metadata_files/regions.json
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
    more:"stuff"
},
,,,,

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

## Example invocation

This assumes that you have cloned this repo into your home directory.

```bash
cd ~/VXingest
outdir="/data/sums_to_cb/rap_ops_130/output/${pid}"
nohup python ${clonedir}/sums_to_cb/run_ingest_threads.py -s /data/sums_to_cb/load_specs/load_spec_metar_sums_rap_ops_130_V01.yaml  -c ~/adb-cb1-credentials -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs &
```

this will create or update a document with the id "MD:V01:METAR:HRRR_OPS:ingest:grib2"
and this document will contain all of the surface, surface, and visibility data defined in the template.

### N1QL metadata queries

These are mostly oriented around cb-surface but they are illustrative.
This will return the min and max fcstValidEpoch for the HRRR model

```bash
select min(mdata.fcstValidEpoch) as mindate, max(fcstValidEpoch) as maxdate from mdata where type="DD" and docType="model" and subset="METAR" and version="V01" and model='HRRR';
```

Alternatively the min and max fcstValidEpochs can be returned quickly with

```bash
select min(meta().id) as minid, max(meta().id) as maxid from mdata where type="DD" and docType="SUMS" and subset="METAR" and version="V01" and model='HRRR';
```

because the ids contain the fcstValidEpochs and the ids will sort by them.
This returns the nuber of METAR contingency tables for the HRRR
select count(meta().id) from mdata where type="DD" and docType="SUMS" and subset="METAR" and version="V01" and model='HRRR';
numrecs: 1248123,

This returns the current epoch
```select floor(NOW_MILLIS()/1000)```

This returns the distinct array of regions for the contingency tables for the HRRR

```bash
select raw array_agg(distinct mdata.region) from mdata where type="DD" and docType="SUMS" and subset="METAR" and version="V01" and model='HRRR';
```

This returns the distinct array of forecast lengths for the contingency tables for the HRRR

```bash
select raw array_agg(distinct mdata.fcstLen) from mdata where type="DD" and docType="SUMS" and subset="METAR" and version="V01" and model='HRRR';
```

This returns the distinct array of thresholds for the contingency tables for the HRRR. This takes a pretty long time,
around a minute.

```bash
SELECT DISTINCT RAW d_thresholds
FROM (SELECT OBJECT_NAMES (mdata.data) AS thresholds
      FROM  mdata 
      WHERE  type="DD" AND docType="SUMS" AND  subset="METAR" AND  version="V01" AND model='HRRR')  AS d
UNNEST d.thresholds AS d_thresholds;
```

## Useful search predicates for retrieving documents

To retrive all the ingest documents for METARS

```bash
type="MD" and docType="ingest" and subset="METAR" and version="V01"
```

To retrieve all the ingest documents for METARS and restrict it to only SUMS ingest documents.

```bash
type="MD" and docType="ingest" and subset="METAR" and version="V01" and subType="SUMS"
```

To retrieve all the SUMS documents for METARS and model HRRR

```bash
type="DD" and docType="SUMS" and subset="METAR" and version="V01" and model="HRRR"
```

To retrieve 10 SUMS documents for HRRR METARS

```bash
select mdata.* from mdata where type="DD" and docType="SUMS" and subset="METAR" and version="V01" and model="HRRR" limit 10
```

To retrieve 10 SUMS documents for HRRR METARS at a specific fcstValidEpoch

```bash
select mdata.* from mdata where type="DD" and docType="SUMS" and subset="METAR" and version="V01" and model="HRRR" and fcstValidEpoch=1516986000 limit 10
```

To count all the METAR model documents where the model is HRR_OPS
If you retrieve these documents and examine them you find that these documents have model
variables at specific fcstValidBeg and are organized by metar station.

```bash
select count(*) from mdata where type="DD" and docType="model" and subset="METAR" and version="V01" and model="HRRR_OPS"
```

To retrieve the metadata document for cb-surface for the HRRR model

```bash
select mdata.* from mdata where type="MD" and docType="mats_gui" and subType="app" and subset="COMMON" and version="V01" and app="cb-surface" and model="HRRR"
```

To retrieve the metadata document for cb-surface for all models

```bash
select mdata.* from mdata where type="MD" and docType="mats_gui" and subType="app" and subset="COMMON" and version="V01" and app="cb-surface"
```
