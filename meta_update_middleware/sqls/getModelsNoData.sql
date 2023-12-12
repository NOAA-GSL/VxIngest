SELECT
    raw m
FROM
    (
        SELECT
            RAW SPLIT(meta().id, ":") [3] AS model
        FROM
            {{vxDBTARGET}}
        WHERE
            meta().id LIKE "MD:matsGui:{{vxAPP}}:%25:COMMON:V01"
            AND type = "MD"
            AND docType = "matsGui"
            AND version = "V01"
        ORDER BY
            model
    ) AS m
WHERE
    m not IN (
        select
            distinct raw model
        from
            vxdata._default.METAR
        where
            type = "DD"
            and docType = "{{vxDOCTYPE}}"
            and subDocType = "{{vxSUBDOCTYPE}}"
            and version = "V01"
        order by
            model
    )