UPDATE
    vxdata._default.METAR
SET
    thresholds = (
        SELECT
            DISTINCT RAW d_thresholds
        FROM
            (
                SELECT
                    OBJECT_NAMES(object_names_t.data) AS thresholds
                FROM
                    vxdata._default.METAR AS object_names_t
                WHERE
                    object_names_t.type = 'DD'
                    AND object_names_t.docType = 'CTC'
                    AND object_names_t.subDocType = 'CEILING'
                    AND object_names_t.version = 'V01'
                    AND object_names_t.model = '${model}'
            ) AS d UNNEST d.thresholds AS d_thresholds
    ),
    fcstLens =(
        SELECT
            DISTINCT VALUE fl.fcstLen
        FROM
            vxdata._default.METAR as fl
        WHERE
            fl.type = 'DD'
            AND fl.docType = 'CTC'
            AND fl.subDocType = 'CEILING'
            AND fl.version = 'V01'
            AND fl.model = '${model}'
        ORDER BY
            fl.fcstLen
    ),
    regions =(
        SELECT
            DISTINCT VALUE rg.region
        FROM
            vxdata._default.METAR as rg
        WHERE
            rg.type = 'DD'
            AND rg.docType = 'CTC'
            AND rg.subDocType = 'CEILING'
            AND rg.version = 'V01'
            AND rg.model = '${model}'
        ORDER BY
            r.METAR.region
    ),
    --if exists use that value else use model name
    displayText =(
        SELECT
            raw CASE
                WHEN m.standardizedModelList.$ { model } IS NOT NULL THEN m.standardizedModelList.$ { model }
                ELSE "${model}"
            END
        FROM
            vxdata._default.METAR AS m USE KEYS "MD:matsAux:COMMON:V01"
    ) [0],
    --if it exists in primaryModelOrders should be 1 else use 2
    displayCategory =(
        SELECT
            raw CASE
                WHEN m.primaryModelOrders.$ { model } IS NOT NULL THEN 1
                ELSE 2
            END
        FROM
            vxdata._default.METAR AS m USE KEYS "MD:matsAux:COMMON:V01"
    ) [0],
    --if it exists in document use that value else use the mindx i.e.
    -- If the display order is discovered below it will be category 1 and the order comes from the document
    -- ELSE set the order to the index of the model in models_requiring_metadata and set category to 2
    displayOrder =(
        WITH k AS (
            SELECT
                RAW m.standardizedModelList.$ { model }
            FROM
                vxdata._default.METAR AS m USE KEYS "MD:matsAux:COMMON:V01"
        )
        SELECT
            RAW CASE
                WHEN m.primaryModelOrders.[k[0]].m_order IS NOT NULL
        THEN m.primaryModelOrders.[k[0]].m_order
        ELSE ${mindx}
        END
        FROM vxdata._default.METAR AS m USE KEYS "MD:matsAux:COMMON:V01"
       )[0],
                mindate =(
                    SELECT
                        RAW MIN(mt.fcstValidEpoch) AS mintime
                    FROM
                        vxdata._default.METAR AS mt
                    WHERE
                        mt.type = 'DD'
                        AND mt.docType = 'CTC'
                        AND mt.subDocType = 'CEILING'
                        AND mt.version = 'V01'
                        AND mt.model = '${model}'
                ) [0],
                maxdate =(
                    SELECT
                        RAW MAX(mat.fcstValidEpoch) AS maxtime
                    FROM
                        vxdata._default.METAR AS mat
                    WHERE
                        mat.type = 'DD'
                        AND mat.docType = 'CTC'
                        AND mat.subDocType = 'CEILING'
                        AND mat.version = 'V01'
                        AND mat.model = '${model}'
                ) [0],
                numrecs =(
                    SELECT
                        RAW COUNT(META().id)
                    FROM
                        vxdata._default.METAR AS n
                    WHERE
                        n.type = 'DD'
                        AND n.docType = 'CTC'
                        AND n.subDocType = 'CEILING'
                        AND n.version = 'V01'
                        AND n.model = '${model}'
                ) [0],
                updated =(
                    SELECT
                        RAW FLOOR(NOW_MILLIS() / 1000)
                ) [0]
                WHERE
                    type = 'MD'
                    AND docType = 'matsGui'
                    AND subset = 'COMMON'
                    AND version = 'V01'
                    AND app = 'cb-ceiling'
                    AND META().id = 'MD:matsGui:cb-ceiling:${model}:COMMON:V01'