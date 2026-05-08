SELECT DISTINCT RAW (SPLIT(meta().id, ":") [3]) model
FROM {{vxDBTARGET}}
WHERE type = "DD"
    AND version = "V01"
    AND docType = "{{vxDOCTYPE}}"
    AND subDocType = "{{vxSUBDOCTYPE}}"
ORDER BY model