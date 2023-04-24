SELECT
    { { stationNamesList } }
FROM
    `vxdata`._default.METAR AS models
WHERE
    type = "DD"
    AND docType = "model"
    AND model = {{model}}
    AND fcstLen = 6
    AND version = "V01"
    AND fcstValidEpoch = { { fcstValidEpoch } }