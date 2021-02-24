CREATE INDEX `adv_type_subset_docType_fcstValidEpoch` ON `mdata`(`type`,`docType`,`fcstValidEpoch`) WHERE (`subset`="METAR") WITH {"defer_build":true};
CREATE INDEX `adv_type_subset_docType_version` ON `mdata`(`type`,`docType`,`version`) WHERE (`subset`="METAR") WITH {"defer_build":true};
CREATE INDEX `adv_type_subset_docType_version_fcstValidEpoch` ON `mdata`(`type`,`docType`,`version`,`fcstValidEpoch`) WHERE (`subset`="METAR") WITH {"defer_build":true};
