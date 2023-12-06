SELECT
    DISTINCT RAW (SPLIT(meta().id, ":") [3]) model
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND docType = "{{vxDOCTYPE}}"
    AND subDocType = "{{vxSUBDOCTYPE}}"
    AND version = "V01"
order by
    model