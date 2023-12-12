select
    raw SPLIT(meta().id, ":") [3]
FROM
    {{vxDBTARGET}}
WHERE
    type = "MD"
    AND docType = "matsGui"
    AND subset = "COMMON"
    AND version = "V01"
    AND app = "{{vxAPP}}"
    AND META().id LIKE "MD:matsGui:{{vxAPP}}:%25:COMMON:V01"