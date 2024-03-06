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
NOTE: If no specific credentails file is given on the command-line, meta-update
will look for and use ~/credentials

meta-update picks up Couchbase conection information from this file, example below
Please note that the cb_user and cb_password values should be replaced with actual values
cb_host: couchbase://adb-cb1.gsd.esrl.noaa.gov
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
## cd /home/amb-verif/VxIngest/meta_update_middleware
go build .
## to update meta-data using ~/credentials, ./settings.json for all apps
go run .
## to update meta-data with specific credentials,settings and/or for a specific app (ceiling)
go run . -c ~/credentials -s ./settings.json -a ceiling


## contab (add the following lines to amb-verif contab on adb-cb1)
# run new matadata update (for Couchbase) for ceiling
0 4 * * * cd /home/amb-verif/VxIngest/meta_update_middleware; ./meta-update -c /home/amb-verif/credentials -s ./settings.json -a ceiling > /home/amb-verif/logs/cron-metadata-update-ceiling-`date +\%s`.out 2>&1

# run new matadata update (for Couchbase) for visibility
0 5 * * * cd /home/amb-verif/VxIngest/meta_update_middleware; ./meta-update -c /home/amb-verif/credentials -s ./settings.json -a visibility > /home/amb-verif/logs/cron-metadata-update-visibility-`date +\%s`.out 2>&1

# run new matadata update (for Couchbase) for surface
0 6 * * * cd /home/amb-verif/VxIngest/meta_update_middleware; ./meta-update -c /home/amb-verif/credentials -s ./settings.json -a surface > /home/amb-verif/logs/cron-metadata-update-surface-`date +\%s`.out 2>&1

