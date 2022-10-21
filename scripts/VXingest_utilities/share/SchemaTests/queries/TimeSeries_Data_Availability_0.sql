SELECT m0data.name AS STA_ID, m0.fcstValidEpoch AS AVTIME,
MIN(m0.fcstValidEpoch) AS min_secs,
MAX(m0.fcstValidEpoch) AS max_secs,
COUNT(DISTINCT m0.fcstValidEpoch) AS N_times,
SUM(CASE WHEN odata.name IS NOT MISSING THEN 1 ELSE 0 END) AS ObsNameCount,
SUM(CASE WHEN m0data.name IS NOT MISSING THEN 1 ELSE 0 END) AS ModelNameCount
 FROM mdata AS m0 USE INDEX (ix_subset_version_model_fcstLen_fcstValidEpoc)
   JOIN mdata AS o USE INDEX(adv_fcstValidEpoch_docType_subset_version_type) ON o.fcstValidEpoch = m0.fcstValidEpoch
 UNNEST o.data AS odata
 UNNEST m0.data AS m0data
   WHERE o.type='DD'
   AND o.docType='obs'
   AND o.subset='METAR'
   AND o.version='V01'
   AND m0.type='DD'
   AND m0.docType='model'
   AND m0.subset='METAR'
   AND m0.version='V01'
   AND m0.model='RAP_OPS_130'
   AND m0data.name IN ['KGKY','KY19','KY49','KY51']
   AND odata.name IN ['KGKY','KY19','KY49','KY51']
   AND o.fcstValidEpoch >= 1655244000
   AND o.fcstValidEpoch <= 1655244000 + 24 * 3600 * 1
   AND m0.fcstValidEpoch >= 1655244000
   AND m0.fcstValidEpoch <= 1655244000 + 24 * 3600 * 1
   AND m0.fcstValidEpoch = o.fcstValidEpoch
   AND m0.fcstLen = 12
 GROUP BY m0data.name, m0.fcstValidEpoch
 ORDER BY m0.fcstValidEpoch;
 