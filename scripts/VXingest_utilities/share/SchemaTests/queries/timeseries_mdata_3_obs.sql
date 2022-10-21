SELECT  m0.fcstValidEpoch AS AVTIME,
 m0.stations.KGKY, m0.stations.KY19, m0.stations.KY49, m0.stations.KY51 
FROM
    `mdatatest0`._default.obs AS m0
WHERE
    m0.fcstValidEpoch >= 1655244000
    AND m0.fcstValidEpoch <= 1655244000  + 24 * 3600 * 1
ORDER BY m0.fcstValidEpoch