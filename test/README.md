# test notes
#### data
The test data includes
* a collection of VSDB files in test/data_files for testing the load_xml (which derives a file list)
* a few load_spec... files for testing load_spec's in xml and yaml
* a data_file/MD::V01::METAR::HRRR_OPS::ceiling::obs.json which is a couchbase metadata document
that is referenced in the load_spec_gsd.yaml load spec and is used by the 
test_gsd_ingest_manager test.

The unit tests assume that you have a local i.e. 127.0.0.1:8091 couchbase server installed.

