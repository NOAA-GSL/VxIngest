SELECT  m0.fcstValidEpoch AS AVTIME,
 m0.stations.KGKY, m0.stations.KY19, m0.stations.KY49, m0.stations.KY51 
FROM
    `mdatatest0`._default.model AS m0
WHERE
    m0.idx0 = "DD:METAR:V01:HRRR_OPS"
    AND m0.fcstValidEpoch >= 1655242200
    AND m0.fcstValidEpoch <= 1655242200 + 24 * 3600 * 3
    AND m0.fcstLen = 12
ORDER BY m0.fcstValidEpoch