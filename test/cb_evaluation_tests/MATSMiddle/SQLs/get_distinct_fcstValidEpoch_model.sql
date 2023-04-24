SELECT DISTINCT fcstValidEpoch
FROM
    `vxdata`._default.METAR
WHERE
    type = "DD"
    AND docType = "model"
    AND model = "HRRR_OPS"
    AND fcstLen = 6
    AND version = "V01"
    AND fcstValidEpoch BETWEEN 1662249600
    AND 1664841600