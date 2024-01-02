# Indexes

## Overview

### Cluster roles

#### ingest / development
The standalone cluster on adb-cb1 is used for ingest data creation and import. The ingest processes query this cluster and the import processes import to this cluster.
#### production
The production cluster is what the apps read. This cluster is populated by xdcr from the ingest cluster

## Index Creation Scripts
We are trying to keep an up to date N1QL script for the standalone cluster and a different one for the production cluster (they are not the same index set) that contain the index creation statements necessary to recreate a proper set of indexes for either of those environments, or for a new environment that has one of thse roles. 

There are three index creation scripts

- mats_metadata_and_indexes/index_creation_scripts/create_indexes-0-replicas.n1ql
  This is for the standalone server since it cannot support index replication (it only has one node)
  Copy the contents of this N1QL script and either execute it in the query window of the UI of the
  standalone server or execute it with the command line interface cbq.
  After that you must also execute the N1QL in the build_indexes.n1ql script.
- mats_metadata_and_indexes/index_creation_scripts/create_indexes-2-replicas.n1ql
  This is for the cluster server since it does support index replication (three nodes)
  Copy the contents of this N1QL script and either execute it in the query window of the UI of the
  cluster server or execute it with the command line interface cbq.
  After that you must also execute the N1QL in the build_indexes.n1ql script.
- mats_metadata_and_indexes/index_creation_scripts/create_fts_search_stations_index.sh
    This is for creating the full text search index. You can execute it as a bash script from any machine
    that has connectivity to the couchbase server. This script takes one parameter - the full hostname of 
    the couchbase server. It will prompt for the avid user password.

## Index Backups
It is difficult to keep these scripts up to date because it is essentially a manual operation. For that reason we keep a set of backups. These backups are small files that are derived from the curl based admin interface for Couchbase.