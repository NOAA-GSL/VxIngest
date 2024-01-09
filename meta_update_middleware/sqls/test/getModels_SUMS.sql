SELECT
    DISTINCT RAW (SPLIT(meta().id, ":") [3]) model
FROM
    vxdata._default.METAR
WHERE
    type = "DD"
    AND docType = "SUMS"
    AND subDocType = "CEILING"
    AND version = "V01"
order by
    model