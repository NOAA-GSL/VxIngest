SELECT m.mfve AS avtime,
       ARRAY_SUM(stats [*].hit) AS hits,
       ARRAY_SUM(stats [*].miss) AS misses,
       ARRAY_SUM(stats [*].false_alarm) AS fa,
       ARRAY_SUM(stats [*].correct_negative) AS cn,
       ARRAY_SUM(stats [*].total) AS N0
FROM (
    SELECT trimData AS odata,
           ofve
    FROM `mdatatest0`._default.obs AS obs
    LET ofve = obs.fcstValidEpoch,
        trimData = ARRAY d FOR d IN ( [obs.stations.KEWR, obs.stations.KJFK, obs.stations.KJRB, obs.stations.KLDJ, obs.stations.KLGA, obs.stations.KNYC, obs.stations.KTEB] ) END
    WHERE obs.idx0 = "DD:METAR:V01:undefined"
        AND obs.fcstValidEpoch BETWEEN 1662249600 AND 1664841600 ) o,
(
    SELECT trimData AS m0data,
           mfve
    FROM `mdatatest0`._default.model AS m0
    LET mfve = m0.fcstValidEpoch,
        trimData = ARRAY d FOR d IN ( [m0.stations.KEWR, m0.stations.KJFK, m0.stations.KJRB, m0.stations.KLDJ, m0.stations.KLGA, m0.stations.KNYC, m0.stations.KTEB] ) END
    WHERE m0.idx0 = "DD:METAR:V01:HRRR_OPS"
        AND m0.fcstLen = 6
        AND m0.fcstValidEpoch BETWEEN 1662249600 AND 1664841600 ) m
LET stats = ARRAY( FIRST { 'hit' :CASE WHEN mv.Ceiling < 3000.0
        AND ov.Ceiling < 3000.0 THEN 1 ELSE 0 END,
                                          'miss' :CASE WHEN mv.Ceiling < 3000.0
        AND NOT ov.Ceiling < 3000.0 THEN 1 ELSE 0 END,
                                              'false_alarm' :CASE WHEN NOT mv.Ceiling < 3000.0
        AND ov.Ceiling < 3000.0 THEN 1 ELSE 0 END,
                                          'correct_negative' :CASE WHEN NOT mv.Ceiling < 3000.0
        AND NOT ov.Ceiling < 3000.0 THEN 1 ELSE 0 END,
                                              'total' :CASE WHEN mv.Ceiling IS NOT MISSING
        AND ov.Ceiling IS NOT MISSING THEN 1 ELSE 0 END } FOR ov IN o.odata WHEN ov.name = mv.name END ) FOR mv IN m.m0data END
WHERE m.mfve = o.ofve