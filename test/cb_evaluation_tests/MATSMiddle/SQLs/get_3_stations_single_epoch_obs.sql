SELECT
    obs.data.KEWR,
    obs.data.KJFK,
    obs.data.KJRB
FROM
    `vxdata`._default.METAR AS obs
WHERE
    type = "DD"
    AND docType = "obs"
    AND version = "V01"
    AND fcstValidEpoch = 1662508800
 