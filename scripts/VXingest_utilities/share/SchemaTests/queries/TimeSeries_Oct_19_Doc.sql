SELECT
    m.mfve as avtime,
    ARRAY_SUM(stats [*].hit) as hits,
    ARRAY_SUM(stats [*].miss) as misses,
    ARRAY_SUM(stats [*].false_alarm) as fa,
    ARRAY_SUM(stats [*].correct_negative) as cn,
    ARRAY_SUM(stats [*].total) as N0
FROM
    (
        SELECT
            trimData AS odata,
            ofve
        FROM
            mdata LET ofve = mdata.fcstValidEpoch,
            trimData = ARRAY d FOR d IN mdata.data
            WHEN d.name IN ['KEWR','KJFK','KJRB','KLDJ','KLGA','KNYC','KTEB']
    END
WHERE
    mdata.type = "DD"
    AND mdata.docType = 'obs'
    AND mdata.version = "V01"
    AND mdata.subset = "METAR"
    AND mdata.fcstValidEpoch BETWEEN 1662249600
    AND 1664841600
) o,
(
    SELECT
        trimData AS m0data,
        mfve
    FROM
        mdata LET mfve = mdata.fcstValidEpoch,
        trimData = ARRAY d FOR d IN mdata.data
        WHEN d.name IN ['KEWR','KJFK','KJRB','KLDJ','KLGA','KNYC','KTEB']
END
WHERE
    mdata.type = "DD"
    AND mdata.docType = 'model'
    AND model = "HRRR_OPS"
    AND fcstLen = 6
    AND mdata.version = "V01"
    AND mdata.subset = "METAR"
    AND mdata.fcstValidEpoch BETWEEN 1662249600
    AND 1664841600
) m LET stats = ARRAY (
    FIRST { 'hit' :CASE
    WHEN mv.Ceiling < 3000.0
    AND ov.Ceiling < 3000.0 THEN 1
    ELSE 0
END,
'miss' :CASE
WHEN mv.Ceiling < 3000.0
AND NOT ov.Ceiling < 3000.0 THEN 1
ELSE 0
END,
'false_alarm' :CASE
WHEN NOT mv.Ceiling < 3000.0
AND ov.Ceiling < 3000.0 THEN 1
ELSE 0
END,
'correct_negative' :CASE
WHEN NOT mv.Ceiling < 3000.0
AND NOT ov.Ceiling < 3000.0 THEN 1
ELSE 0
END,
'total' :CASE
WHEN mv.Ceiling IS NOT MISSING
AND ov.Ceiling IS NOT MISSING THEN 1
ELSE 0
END } FOR ov IN o.odata
WHEN ov.name = mv.name
END
) FOR mv IN m.m0data
END
WHERE
    m.mfve = o.ofve