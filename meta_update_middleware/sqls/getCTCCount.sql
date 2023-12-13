SELECT
    COUNT(*)
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND docType = "CTC"
    AND subDocType = "CEILING"
    AND version = "V01"