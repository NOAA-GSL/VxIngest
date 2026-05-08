SELECT DISTINCT RAW fl.fcstLen
FROM {{vxDBTARGET}} as fl
WHERE fl.type = 'DD'
    AND fl.version = 'V01'
    AND fl.docType = '{{vxDOCTYPE}}'
    AND fl.subDocType = '{{vxSUBDOCTYPE}}'
    AND fl.model = '{{vxMODEL}}'
ORDER BY fl.fcstLen