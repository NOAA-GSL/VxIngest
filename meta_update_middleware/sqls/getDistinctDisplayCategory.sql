SELECT raw CASE
        WHEN  m.primaryModelOrders.{{vxMODEL}} IS NOT NULL
        THEN 1
        ELSE 2
        END
        FROM {{vxDBTARGET}} AS m
        USE KEYS "MD:matsAux:COMMON:V01"