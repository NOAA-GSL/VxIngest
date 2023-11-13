select
    raw SPLIT(meta().id, ":") [3]
FROM
    vxdata._default.METAR
WHERE
    type = "MD"
    AND docType = "matsGui"
    AND subset = "COMMON"
    AND version = "V01"
    AND app = "cb-ceiling"
    AND META().id LIKE "MD:matsGui:cb-ceiling:%25:COMMON:V01"