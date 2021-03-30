CREATE INDEX `adv_region_fcstValidEpoch_docType_subset_version_type` ON `mdata`(`region`,`fcstValidEpoch`) WHERE ((((`type` = "DD") and (`docType` = "CTC")) and (`subset` = "METAR")) and (`version` = "V01"))  WITH {  "defer_build":true };
CREATE INDEX `idx_type_docType_version_METAR` ON `mdata`(`type`,`docType`,`version`) WHERE (`subset` = "METAR") WITH {  "defer_build":true };
CREATE INDEX `idx_type_docType_version_common` ON `mdata`(`type`,`docType`,`version`) WHERE (`subset` = "COMMON") WITH {  "defer_build":true };
CREATE INDEX `idx_type_docType_version_test` ON `mdata`(`type`,`docType`,`version`) WHERE (`subset` = "TEST") WITH { "defer_build":true };
CREATE INDEX `idx_type_docType_version_fcstValidEpoch_METAR` ON `mdata`(`type`,`docType`,`version`,`fcstValidEpoch`) WHERE (`subset` = "METAR") WITH {  "defer_build":true };

CREATE INDEX `adv_region_fcstValidEpoch_docType_subset_version_type` ON `mdata`(`region`,`fcstValidEpoch`) WHERE ((((`type` = "DD") and (`docType` = "CTC")) and (`subset` = "METAR")) and (`version` = "V01")) WITH {  "defer_build":true };
CREATE INDEX `adv_model_version_type_docType_subset` ON `mdata`(`model`) WHERE ((((`type` = "DD") and (`docType` = "model")) and (`subset` = "METAR")) and (`version` = "V01")) WITH {  "defer_build":true };
CREATE INDEX `adv_model_type_docType_subset_version_fcstValidBeg_fcstValidEpoch` ON `mdata`(`model`,`fcstValidBeg`,`fcstValidEpoch`) WHERE ((((`type` = "DD") and (`docType` = "model")) and (`subset` = "METAR")) and (`version` = "V01")) WITH {  "defer_build":true };
CREATE INDEX `adv_model_type_docType_subset_version` ON `mdata`(`model`) WHERE ((((`docType` = "CTC") and (`subset` = "METAR")) and (`version` = "V01")) and (`type` = "DD")) WITH {  "defer_build":true };
CREATE INDEX `adv_model_subset_version_type_docType_fcstLen` ON `mdata`(`model`,`fcstLen`) WHERE ((((`type` = "DD") and (`docType` = "model")) and (`subset` = "METAR")) and (`version` = "V01")) WITH {  "defer_build":true }
CREATE INDEX `adv_model_region_docType_subset_version_type` ON `mdata`(`model`,`region`) WHERE ((((`subset` = "METAR") and (`version` = "V01")) and (`type` = "DD")) and (`docType` = "CTC")) WITH {  "defer_build":true }
CREATE INDEX `adv_model_fcstLen_subset_version_type_docType` ON `mdata`(`model`,`fcstLen`) WHERE ((((`docType` = "CTC") and (`subset` = "METAR")) and (`version` = "V01")) and (`type` = "DD")) WITH {  "defer_build":true }

BUILD INDEX ON mdata ((SELECT RAW name FROM system:indexes WHERE keyspace_id = 'mdata' AND state = 'deferred'));

