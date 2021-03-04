CREATE INDEX `adv_type_subset_docType_fcstValidEpoch` ON `mdata`(`type`,`docType`,`fcstValidEpoch`) WHERE (`subset`="METAR") WITH {"defer_build":true, "num_replica":2};
CREATE INDEX `adv_type_subset_docType_version` ON `mdata`(`type`,`docType`,`version`) WHERE (`subset`="METAR") WITH {"defer_build":true, "num_replica":2};
CREATE INDEX `adv_type_subset_docType_version_fcstValidEpoch` ON `mdata`(`type`,`docType`,`version`,`fcstValidEpoch`) WHERE (`subset`="METAR") WITH {"defer_build":true, "num_replica":2};
CREATE INDEX `adv_geo_lat_geo_lon_type_docType_subset_version` ON `mdata`((`geo`.`lat`),(`geo`.`lon`)) WHERE ((((`docType` = "station") and (`subset` = "METAR")) and (`version` = "V03")) and (`type` = "DD")){"defer_build":true, "num_replica":2}
BUILD INDEX ON mdata ((SELECT RAW name FROM system:indexes WHERE keyspace_id = 'mdata' AND state = 'deferred'));
