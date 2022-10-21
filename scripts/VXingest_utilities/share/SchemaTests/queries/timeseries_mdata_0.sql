SELECT
    m0.fcstValidEpoch AS AVTIME, m0.stations AS stations,
    MIN(CEIL(3600 * FLOOR((m0.fcstValidEpoch + 1800) / 3600))) AS min_secs,
    MAX(CEIL(3600 * FLOOR((m0.fcstValidEpoch + 1800) / 3600))) AS max_secs,
    COUNT(DISTINCT m0.fcstValidEpoch) AS N_times
FROM
    `mdatatest0`._default.model AS m0
WHERE
    m0.idx0 = "DD:METAR:V01:HRRR_OPS"
    AND m0.fcstValidEpoch >= 1655242200
    AND m0.fcstValidEpoch <= 1655242200 + 24 * 3600 * 3
GROUP BY
    m0.fcstValidEpoch, m0.stations
ORDER BY
    m0.fcstValidEpoch
LIMIT 10;