SELECT
    DISTINCT VALUE fl.fcstLen
FROM
    {{vxDBTARGET}} as fl
WHERE
    fl.type = 'DD'
    AND fl.docType = '{{vxDOCTYPE}}'
    AND fl.subDocType = '{{vxSUBDOCTYPE}}'
    AND fl.version = 'V01'
    AND fl.model = '{{vxMODEL}}'
ORDER BY
    fl.fcstLen