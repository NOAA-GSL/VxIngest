# MATS metadata update middleware

The system relies on a couple of kinds of metadata. Ingest builders use ingest documents to control how an output document is created from its input source. The running system relies on various metadata documents to define what data is available and how it should be repesented in a GUI interface.

## Ingest documents

Ingest documents contain templates that define how a builder should assemble an out document.

### configuration
settings.json
Set the couchbase cluster, bucket, scope, collection and credentials in this file

## updating the metadata
go run meta-update.go

