WITH k AS (
    SELECT
        RAW m.standardizedModelList.{{vxMODEL}}
    FROM
        {{vxDBTARGET}} AS m USE KEYS "MD:matsAux:COMMON:V01"
)
SELECT
    RAW CASE
        WHEN m.primaryModelOrders.[k[0]].m_order IS NOT NULL
        THEN m.primaryModelOrders.[k[0]].m_order
        ELSE {{mindx}}
        END
        FROM {{vxDBTARGET}} AS m USE KEYS "MD:matsAux:COMMON:V01"

