# MATS metadata update middleware
The system relies on a couple of kinds of metadata. Ingest builders use ingest documents to control how an output document is created from its input source. The running system relies on various metadata documents to define what data is available and how it should be repesented in a GUI interface.

## Ingest documents
Ingest documents contain templates that define how a builder should assemble an out document.

# Compile time requirement
Go runtime >= 1.21.3

# Runtime dependencies
Make sure settings.json is in the same folder as the meta-update executable

### configuration
settings.json
Set the couchbase cluster, bucket, scope, collection and credentials in this file
If there is more than one database section in private.databases, the program uses 
databases[0] as target and databases[1] as source.  This is useful if we need to 
test, but the updated meta-data should not be overwritten in the source database.
For example:
    databases[0] set to vxdatatest
    databases[1] set to vxdata
    This will extract meta-data information from vxdata and then update metadata in vxdatatest

## updating the metadata
## cd VxIngest/meta_update_middleware
## to update meta-data using ./settings.json for all apps
go run .
## to update meta-data using an alternate settings file and/or for a specific app (surface)
go run . ./settings.json surface


/Users/gopa.padmanabhan/git/ascend/VxIngest/meta_update_middleware/sqls/test/metadata_new_structure.json