SELECT m0data.name as sta_name,
       COUNT(DISTINCT m0.fcstValidEpoch) N_times,
       MIN(m0.fcstValidEpoch) min_secs,
       MAX(m0.fcstValidEpoch) max_secs,
       SUM(CASE WHEN m0data.Ceiling < 3000.0
               AND odata.Ceiling < 3000.0 THEN 1 ELSE 0 END) AS hit,
       SUM(CASE WHEN m0data.Ceiling < 3000.0
               AND NOT odata.Ceiling < 3000.0 THEN 1 ELSE 0 END) AS fa,
       SUM(CASE WHEN NOT m0data.Ceiling < 3000.0
               AND odata.Ceiling < 3000.0 THEN 1 ELSE 0 END) AS miss,
       SUM(CASE WHEN NOT m0data.Ceiling < 3000.0
               AND NOT odata.Ceiling < 3000.0 THEN 1 ELSE 0 END) AS cn,
       SUM(CASE WHEN m0data.Ceiling IS NOT MISSING
               AND odata.Ceiling IS NOT MISSING THEN 1 ELSE 0 END) AS N0,
       ARRAY_AGG(TO_STRING(m0.fcstValidEpoch) || ';' || CASE WHEN m0data.Ceiling < 3000.0
               AND odata.Ceiling < 3000.0 THEN '1' ELSE '0' END || ';' || CASE WHEN m0data.Ceiling < 3000.0
               AND NOT odata.Ceiling < 3000.0 THEN '1' ELSE '0' END || ';' || CASE WHEN NOT m0data.Ceiling < 3000.0
               AND odata.Ceiling < 3000.0 THEN '1' ELSE '0' END || ';' || CASE WHEN NOT m0data.Ceiling < 3000.0
               AND NOT odata.Ceiling < 3000.0 THEN '1' ELSE '0' END) AS sub_data
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
    AND m0.model='HRRR_OPS'
    AND m0.fcstLen = 6
    AND o.fcstValidEpoch >= 1664236800
    AND o.fcstValidEpoch <= 1664841600
    AND m0.fcstValidEpoch >= 1664236800
    AND m0.fcstValidEpoch <= 1664841600
    AND m0.fcstValidEpoch = o.fcstValidEpoch
	AND m0data.name IN ['KEWR','KJFK','KJRB','KLDJ','KLGA','KNYC','KTEB']
    AND odata.name IN ['KEWR','KJFK','KJRB','KLDJ','KLGA','KNYC','KTEB']
    AND m0data.name = odata.name
GROUP BY m0data.name
ORDER BY m0data.name;