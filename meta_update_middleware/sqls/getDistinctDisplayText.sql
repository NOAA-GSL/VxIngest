SELECT
    raw CASE
        WHEN m.standardizedModelList.{{vxMODEL}} IS NOT NULL THEN m.standardizedModelList.{{vxMODEL}}
        ELSE "{{vxMODEL}}"
    END
FROM
    {{vxDBTARGET}} AS m USE KEYS "MD:matsAux:COMMON:V01"
