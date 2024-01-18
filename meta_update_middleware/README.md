# MATS metadata update middleware
Original notes:
---------------
The system relies on a couple of kinds of metadata. Ingest builders use ingest documents to control how an output document is created from its input source. The running system relies on various metadata documents to define what data is available and how it should be repesented in a GUI interface.
Updated metadata notes:
-----------------------
The new metadata document consolidates MD for all models in one JSON document.
See meta_update_middleware/metadata_new_structure_1.json foe an example
If docType == SUMS in the settings.json section for a particular app, instead of an array of
thresholds, you will get an array of variables.

## Ingest documents
Ingest documents contain templates that define how a builder should assemble an out document.

# Compile time requirement
Go runtime >= 1.21.3

# Runtime dependencies
Make sure settings.json is in the same folder as the meta-update executable

### configuration
credentials file
meta-update picks up Couchbase conection information from this file, example below
Please note that the cb_user and cb_password values should be replaced with actual values
cb_host: adb-cb1.gsd.esrl.noaa.gov
cb_user: ***
cb_password: ***
cb_bucket: vxdata
cb_scope: _default
cb_collection: METAR

To pint to cluster, use
cb_host: adb-cb2.gsd.esrl.noaa.gov,adb-cb3.gsd.esrl.noaa.gov,adb-cb4.gsd.esrl.noaa.gov

settings.json
Defines each app and its docType and subDocTypeß

## updating the metadata
## cd VxIngest/meta_update_middleware
## to update meta-data using ~/credentials, ./settings.json for all apps
go run .
## to update meta-data with specific credentials,settings and/or for a specific app (ceiling)
go run . -c ~/credentials -s ./settings.json -a ceiling


