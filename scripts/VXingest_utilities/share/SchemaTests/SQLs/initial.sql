CREATE INDEX adv_docType_fcstLen_model_fcstValidEpoch_type_version_model ON `default`:`mdatatest`.`_default`.`METAR`(`model`, `fcstLen`, `fcstValidEpoch`) WHERE ((`docType` = 'model') AND (`type` = 'DD') and (`version` = 'V01'))
CREATE INDEX adv_docType_fcstValidEpoch_type_version_obs ON `default`:`mdatatest`.`_default`.`METAR`(`fcstValidEpoch`) WHERE ((`docType` = 'obs') AND (`type` = 'DD') and (`version` = 'V01'))


CREATE INDEX model_object_names_stations ON `default`:`mdata`.`_default`.`model`(object_names(`stations`))
CREATE INDEX obs_object_names_stations ON `default`:`mdata`.`_default`.`obs`(object_names(`stations`))

SELECT count(*) FROM `mdata`._default.model  WHERE object_names(stations) IS NOT NULL;
SELECT count(*) FROM `mdata`._default.obs  WHERE object_names(stations) IS NOT NULL;
SELECT count(DISTINCT object_names(stations)) FROM `mdata`._default.model;
SELECT count(DISTINCT object_names(stations)) FROM `mdata`._default.obs;

CREATE INDEX model_idx0_fcstLen_fcstValidEpoch ON `default`:`mdata`.`_default`.`model`(`idx0`,`fcstLen`,`fcstValidEpoch`)

CREATE INDEX model_stations_name ON `mdata`._default.model(stations.name);
CREATE INDEX obs_stations_name ON `mdata`._default.obs(stations.name);

CREATE INDEX obs_stations ON `mdata`._default.obs(stations);
CREATE INDEX model_stations ON `mdata`._default.model(stations);

select count(*) from mdata._default.obs where stations.length > 0;
select count(*) from mdata._default.model where stations.length > 0;
