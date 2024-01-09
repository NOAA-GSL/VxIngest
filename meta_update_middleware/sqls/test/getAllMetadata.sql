select
    raw md
FROM
    vxdata._default.METAR md
WHERE
    type = "MD"
    AND docType = "matsGui"
    AND subset = "COMMON"
    AND version = "V01"