SELECT  m0.fcstValidEpoch AS AVTIME,
 m0.stations.KGKY, m0.stations.KY19, m0.stations.KY49, m0.stations.KY51 
FROM
    `mdatatest0`._default.model AS m0
WHERE
    REGEXP_CONTAINS(m0.idx0, "DD:METAR:V01:*")
    AND m0.fcstValidEpoch >= 1655244000
    AND m0.fcstValidEpoch <= 1655244000 + 24 * 3600 * 1
    AND m0.fcstLen = 12
ORDER BY m0.fcstValidEpoch