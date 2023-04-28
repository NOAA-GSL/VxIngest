SELECT
    models.data.KEWR,
    models.data.KJFK,
    models.data.KJRB
FROM
    `vxdata`._default.METAR AS models
WHERE
    type = "DD"
    AND docType = "model"
    AND model = "HRRR_OPS"
    AND fcstLen = 6
    AND version = "V01"
    AND fcstValidEpoch = 1662508800
 