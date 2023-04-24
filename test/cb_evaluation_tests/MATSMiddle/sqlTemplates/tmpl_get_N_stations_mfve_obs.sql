SELECT
    {{stationNamesList}}
FROM
    `vxdata`._default.METAR AS obs
WHERE
    type = "DD"
    AND docType = "obs"
    AND version = "V01"
    AND fcstValidEpoch = {{fcstValidEpoch}}