
CREATE INDEX `idx_type_docType_version_METAR` ON `mdata`(`type`,`docType`,`version`) WHERE (`subset` = "METAR") WITH {  "defer_build":true, "num_replica":2};
CREATE INDEX `idx_type_docType_version_common` ON `mdata`(`type`,`docType`,`version`) WHERE (`subset` = "COMMON") WITH {  "defer_build":true, "num_replica":2};
CREATE INDEX `idx_type_docType_version_test` ON `mdata`(`type`,`docType`,`version`) WHERE (`subset` = "TEST") WITH { "defer_build":true, "num_replica":2};
CREATE INDEX `idx_type_docType_version_fcstValidEpoch_METAR` ON `mdata`(`type`,`docType`,`version`,`fcstValidEpoch`) WHERE (`subset` = "METAR") WITH {  "defer_build":true, "num_replica":2};
BUILD INDEX ON mdata ((SELECT RAW name FROM system:indexes WHERE keyspace_id = 'mdata' AND state = 'deferred'));
