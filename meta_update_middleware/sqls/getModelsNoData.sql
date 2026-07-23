WITH model_names AS (
    SELECT DISTINCT RAW COALESCE(m.name, m.model)
    FROM vxdata._default.METAR AS d UNNEST d.models AS m
    WHERE META(d).id IN [
        "MD:matsGui:ceiling:COMMON:V01",
        "MD:matsGui:visibility:COMMON:V01",
        "MD:matsGui:surface:COMMON:V01"
    ]
        AND COALESCE(m.name, m.model) IS NOT NULL
        AND COALESCE(m.name, m.model) != ""
)
SELECT RAW mn
FROM model_names AS mn
WHERE NOT EXISTS (
        SELECT 1
        FROM vxdata._default.METAR AS mt
        WHERE mt.type = "DD"
            AND mt.docType IN ["CTC", "SUMS"]
            AND mt.version = "V01"
            AND mt.model = mn
            AND mt.subset = "METAR"
        LIMIT 1
    )
ORDER BY mn;