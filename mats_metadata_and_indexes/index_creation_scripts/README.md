# How to build the indexes

## Index Creation Scripts

There are three index creation scripts

- mats_metadata_and_indexes/index_creation_scripts/create_indexes-0-replicas.n1ql
  This is for the standalone server since it cannot support index replication (only one node)
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
