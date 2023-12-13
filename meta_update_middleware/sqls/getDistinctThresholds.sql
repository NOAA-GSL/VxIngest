SELECT
    DISTINCT OBJECT_NAMES(object_names_t.data) AS thresholds
FROM
    {{vxDBTARGET}} AS object_names_t
WHERE
    object_names_t.type = 'DD'
    AND object_names_t.docType = '{{vxDOCTYPE}}'
    AND object_names_t.subDocType = '{{vxSUBDOCTYPE}}'
    AND object_names_t.version = 'V01'
    AND object_names_t.model = '{{vxMODEL}}'