SELECT m.mfve AS avtime,
       ARRAY_SUM(stats [*].hit) AS hit,
       ARRAY_SUM(stats [*].miss) AS miss,
       ARRAY_SUM(stats [*].false_alarm) AS fa,
       ARRAY_SUM(stats [*].correct_negative) AS cn,
       ARRAY_SUM(stats [*].total) AS N0,
       ARRAY_COUNT(ARRAY_DISTINCT(stats [*].fve)) AS N_times,
       ARRAY_SORT(stats [*].sub) AS sub_data
FROM (
    SELECT sdu.ovfe AS ovfe,
           ARRAY_AGG(sdu) AS data
    FROM (
        SELECT stationData
        FROM vxdata._default.METAR AS obs
        LET ofve = obs.fcstValidEpoch,
            stationData = ARRAY OBJECT_ADD(d, 'ofve', ofve) FOR d IN ( [obs.data.K04V,obs.data.K0CO,obs.data.K1JW,obs.data.K1LW,obs.data.K1MW,obs.data.K1NW,obs.data.K1OW,obs.data.K1V6,obs.data.K20V,obs.data.K2V5,obs.data.K33V,obs.data.K4BM,obs.data.K4V0,obs.data.K5SM,obs.data.K7BM,obs.data.KABH,obs.data.KAEJ,obs.data.KAFF,obs.data.KAJZ,obs.data.KAKO,obs.data.KALS,obs.data.KANK,obs.data.KAPA,obs.data.KASE,obs.data.KBDU,obs.data.KBJC,obs.data.KBKF,obs.data.KCAG,obs.data.KCCU,obs.data.KCEZ,obs.data.KCFO,obs.data.KCOS,obs.data.KCPW,obs.data.KCWN,obs.data.KDEN,obs.data.KDRO,obs.data.KEEO,obs.data.KEGE,obs.data.KEIK,obs.data.KFCS,obs.data.KFLY,obs.data.KFMM,obs.data.KFNL,obs.data.KGJT,obs.data.KGNB,obs.data.KGUC,obs.data.KGXY,obs.data.KHDN,obs.data.KHEQ,obs.data.KITR,obs.data.KLAA,obs.data.KLHX,obs.data.KLIC,obs.data.KLMO,obs.data.KLXV,obs.data.KMNH,obs.data.KMTJ,obs.data.KMYP,obs.data.KPSO,obs.data.KPUB,obs.data.KRCV,obs.data.KRIL,obs.data.KSBS,obs.data.KSHM,obs.data.KSPD,obs.data.KSTK,obs.data.KTAD,obs.data.KTEX,obs.data.KVTP] ) END
        WHERE type = "DD"
            AND docType = "obs"
            AND version = "V01"
            AND obs.fcstValidEpoch BETWEEN 1668272400 AND 1670864400 ) sd
    UNNEST sd.stationData sdu
    GROUP BY sdu.ovfe
    ORDER BY sdu.ovfe ) o,
(
    SELECT sdu.mfve AS mfve,
           ARRAY_AGG(sdu) AS data
    FROM (
        SELECT modelData
        FROM vxdata._default.METAR AS models
        LET mfve = models.fcstValidEpoch,
            modelData = ARRAY OBJECT_ADD(d, 'mfve', mfve) FOR d IN ( [models.data.K04V,models.data.K0CO,models.data.K1JW,models.data.K1LW,models.data.K1MW,models.data.K1NW,models.data.K1OW,models.data.K1V6,models.data.K20V,models.data.K2V5,models.data.K33V,models.data.K4BM,models.data.K4V0,models.data.K5SM,models.data.K7BM,models.data.KABH,models.data.KAEJ,models.data.KAFF,models.data.KAJZ,models.data.KAKO,models.data.KALS,models.data.KANK,models.data.KAPA,models.data.KASE,models.data.KBDU,models.data.KBJC,models.data.KBKF,models.data.KCAG,models.data.KCCU,models.data.KCEZ,models.data.KCFO,models.data.KCOS,models.data.KCPW,models.data.KCWN,models.data.KDEN,models.data.KDRO,models.data.KEEO,models.data.KEGE,models.data.KEIK,models.data.KFCS,models.data.KFLY,models.data.KFMM,models.data.KFNL,models.data.KGJT,models.data.KGNB,models.data.KGUC,models.data.KGXY,models.data.KHDN,models.data.KHEQ,models.data.KITR,models.data.KLAA,models.data.KLHX,models.data.KLIC,models.data.KLMO,models.data.KLXV,models.data.KMNH,models.data.KMTJ,models.data.KMYP,models.data.KPSO,models.data.KPUB,models.data.KRCV,models.data.KRIL,models.data.KSBS,models.data.KSHM,models.data.KSPD,models.data.KSTK,models.data.KTAD,models.data.KTEX,models.data.KVTP] ) END
        WHERE type = "DD"
            AND docType = "model"
            AND model = 'HRRR_OPS'
            AND fcstLen = 6
            AND version = "V01"
            AND models.fcstValidEpoch BETWEEN 1668272400 AND 1670864400 ) sd
    UNNEST sd.modelData sdu
    GROUP BY sdu.mfve
    ORDER BY sdu.mfve ) m
LET stats = ARRAY( FIRST { 'hit' :CASE WHEN mv.Ceiling < 500.0
        AND ov.Ceiling < 500.0 THEN 1 ELSE 0 END,
                                          'miss' :CASE WHEN NOT mv.Ceiling < 500.0
        AND ov.Ceiling < 500.0 THEN 1 ELSE 0 END,
                                          'false_alarm' :CASE WHEN mv.Ceiling < 500.0
        AND NOT ov.Ceiling < 500.0 THEN 1 ELSE 0 END,
                                              'correct_negative' :CASE WHEN NOT mv.Ceiling < 500.0
        AND NOT ov.Ceiling < 500.0 THEN 1 ELSE 0 END,
                                              'total' :CASE WHEN mv.Ceiling IS NOT MISSING
        AND ov.Ceiling IS NOT MISSING THEN 1 ELSE 0 END,
                                                'fve': mv.mfve,
                                                'sub': TO_STRING(mv.mfve) || ';' || CASE WHEN mv.Ceiling < 500.0
        AND ov.Ceiling < 500.0 THEN '1' ELSE '0' END || ';' || CASE WHEN mv.Ceiling < 500.0
        AND NOT ov.Ceiling < 500.0 THEN '1' ELSE '0' END || ';' || CASE WHEN NOT mv.Ceiling < 500.0
        AND ov.Ceiling < 500.0 THEN '1' ELSE '0' END || ';' || CASE WHEN NOT mv.Ceiling < 500.0
        AND NOT ov.Ceiling < 500.0 THEN '1' ELSE '0' END } FOR ov IN o.data WHEN ov.ofve = mv.mfve
        AND ov.name = mv.name END ) FOR mv IN m.data END
