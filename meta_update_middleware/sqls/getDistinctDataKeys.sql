SELECT DISTINCT RAW distinct_data_keys
FROM {{vxDBTARGET}} AS m UNNEST OBJECT_NAMES(m.data) AS distinct_data_keys
WHERE m.type = "DD"
    AND m.docType = '{{vxDOCTYPE}}'
    AND m.subDocType = '{{vxSUBDOCTYPE}}'
    AND m.version = "V01"
    AND m.model = '{{vxMODEL}}';