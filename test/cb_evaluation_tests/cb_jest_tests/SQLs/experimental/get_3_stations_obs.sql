SELECT obs.data.KEWR,
       obs.data.KJFK,
       obs.data.KJRB data
FROM `vxdata`._default.METAR AS obs
WHERE type = "DD"
    AND docType = "obs"
    AND version = "V01"
    AND obs.fcstValidEpoch BETWEEN 1662249600 AND 1664841600