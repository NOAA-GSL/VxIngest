#!/bin/sh
gitroot=$(git rev-parse --show-toplevel)
if [ "$gitroot" != "$(pwd)" ]; then
    echo "$(pwd) is not a git root directory: cd to the clone root of VxIngest"
    exit
fi

if [ $# -ne 1 ]; then
    echo "Usage $0 credentials-file"
    exit 1
fi
if [[ ! -f "$1" ]]; then
    echo "$1 is not a valid file - exiting"
    exit 1
fi

credentials=$1
m_host=$(grep mysql_host ${credentials} | awk '{print $2}')
m_user=$(grep mysql_user ${credentials} | awk '{print $2}')
m_password=$(grep mysql_password ${credentials} | awk '{print $2}')
cb_host=$(grep cb_host ${credentials} | awk '{print $2}')
cb_user=$(grep cb_user ${credentials} | awk '{print $2}')
cb_pwd=$(grep cb_password ${credentials} | awk '{print $2}')
cred="${cb_user}:${cb_pwd}"
#get needed models
models_requiring_metadata=($(curl -s -u ${cred} http://${cb_host}:8093/query/service -d statement='SELECT DISTINCT RAW (SPLIT(meta().id,":")[3]) model FROM vxdata._default.METAR WHERE type="DD" AND docType="SUMS" AND subDocType="SURFACE" AND version="V01" order by model' | jq -r '.results[]'))
echo "------models_requiring metadata--${models_requiring_metadata[@]}"
#get models having metadata but no data (remove metadata for these)
#(note 'like %' is changed to 'like %25')
remove_metadata_for_models=($(curl -s -u ${cred} http://${cb_host}:8093/query/service -d statement='SELECT raw m FROM (SELECT RAW SPLIT(meta().id,":")[3] AS model FROM vxdata._default.METAR WHERE META().id LIKE "MD:matsGui:cb-surface:%25:COMMON:V01" AND type="MD" AND docType="matsGui" AND version="V01" ORDER BY model) AS m WHERE m not IN (select distinct raw model from vxdata._default.METAR where type="DD" and docType="SUMS" and subDocType="SURFACE" and version="V01" order by model)' | jq -r '.results[]'))
echo "------models not requiring metadata (remove metadata)--${remove_metadata_for_models[@]}" # process models
# remove metadata for models with no data
for model in ${remove_metadata_for_models[@]}; do
     cmd="delete FROM vxdata._default.METAR WHERE type='MD' AND docType='matsGui' AND subset='COMMON' AND version='V01' AND app='cb-surface' AND META().id='MD:matsGui:cb-surface:${model}:COMMON:V01'"
     echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=${cmd}\""
     curl -s -u ${cred} http://${cb_host}:8093/query/service -d "statement=${cmd}"
done
# initialize the metadata for the models for which the metadata does not exist
models_with_existing_metadata=($(curl -s -u ${cred} http://${cb_host}:8093/query/service -d statement='select raw SPLIT(meta().id,":")[3]  FROM vxdata._default.METAR WHERE type="MD" AND docType="matsGui" AND subset="COMMON" AND version="V01" AND app="cb-surface" AND META().id LIKE "MD:matsGui:cb-surface:%25:COMMON:V01"' | jq -r '.results[]'))
for m in ${models_requiring_metadata[@]}; do
    if [[ ! " ${models_with_existing_metadata[@]} " =~ " ${m} " ]]; then
        # initialize the metadata for this model - it will get updated in the next step
        echo "initializing metadata for model $m"
        cmd=$(
            cat <<-%EODinsertmetadata
            INSERT INTO vxdata._default.METAR (KEY, VALUE)
            VALUES ("MD:matsGui:cb-surface:${m}:COMMON:V01",
              {"id": "MD:matsGui:cb-surface:${m}:COMMON:V01",
                "type": "MD",
                "docType": "matsGui",
                "app": "cb-surface",
                "model": "$m",
                "subset": "COMMON",
                "version": "V01",
                "displayText": "$m",
                "displayCategory": 1,
                "displayOrder": 1,
                "mindate": "1",
                "maxdate": "1",
                "numrecs": "1",
                "updated": "1",
                "regions": [],
                "fcstLens": []})
%EODinsertmetadata
        )
        echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=${cmd}\""
        curl -s -u ${cred} http://${cb_host}:8093/query/service -d "statement=${cmd}"
    fi
done

# get a sorted list of all the models_requiring_metadata
# now update all the metdata for all the models that require it
for mindx in "${!models_requiring_metadata[@]}"; do
    model="${models_requiring_metadata[$mindx]}"
    cmd=$(
        cat <<-%EODupdatemetadata
    UPDATE vxdata._default.METAR
    SET
    fcstLens=(
    SELECT DISTINCT VALUE fl.fcstLen
    FROM vxdata._default.METAR as fl
    WHERE fl.type='DD'
        AND fl.docType='SUMS'
        AND fl.subDocType='SURFACE'
        AND fl.version='V01'
        AND fl.model='${model}'
        ORDER BY fl.fcstLen),
    regions=(
    SELECT DISTINCT VALUE rg.region
    FROM vxdata._default.METAR as rg
    WHERE rg.type='DD'
        AND rg.docType='SUMS'
        AND rg.subDocType='SURFACE'
        AND rg.version='V01'
        AND rg.model='${model}'
    ORDER BY r.METAR.region),
    --if exists use that value else use model name
    displayText=(
        SELECT raw CASE
        WHEN  m.standardizedModelList.${model} IS NOT NULL
        THEN m.standardizedModelList.${model}
        ELSE "${model}"
        END
        FROM vxdata._default.METAR AS m
        USE KEYS "MD:matsAux:COMMON:V01"
        )[0],
    --if it exists in primaryModelOrders should be 1 else use 2
    displayCategory=(
        SELECT raw CASE
        WHEN  m.primaryModelOrders.${model} IS NOT NULL
        THEN 1
        ELSE 2
        END
        FROM vxdata._default.METAR AS m
        USE KEYS "MD:matsAux:COMMON:V01"
        )[0],
    --if it exists in document use that value else use the mindx i.e.
    -- If the display order is discovered below it will be category 1 and the order comes from the document
    -- ELSE set the order to the index of the model in models_requiring_metadata and set category to 2
    displayOrder=(
        WITH k AS (
            SELECT RAW m.standardizedModelList.${model}
            FROM vxdata._default.METAR AS m USE KEYS "MD:matsAux:COMMON:V01" )
        SELECT RAW CASE
        WHEN m.primaryModelOrders.[k[0]].m_order IS NOT NULL
        THEN m.primaryModelOrders.[k[0]].m_order
        ELSE ${mindx}
        END
        FROM vxdata._default.METAR AS m USE KEYS "MD:matsAux:COMMON:V01"
       )[0],
    mindate=(
        SELECT RAW MIN(mt.fcstValidEpoch) AS mintime
        FROM vxdata._default.METAR AS mt
        WHERE mt.type='DD'
            AND mt.docType='SUMS'
            AND mt.subDocType='SURFACE'
            AND mt.version='V01'
            AND mt.model='${model}')[0],
    maxdate=(
        SELECT RAW MAX(mat.fcstValidEpoch) AS maxtime
        FROM vxdata._default.METAR AS mat
        WHERE mat.type='DD'
            AND mat.docType='SUMS'
            AND mat.subDocType='SURFACE'
            AND mat.version='V01'
            AND mat.model='${model}')[0],
    numrecs=(
        SELECT RAW COUNT(META().id)
        FROM vxdata._default.METAR AS n
        WHERE n.type='DD'
            AND n.docType='SUMS'
            AND n.subDocType='SURFACE'
            AND n.version='V01'
            AND n.model='${model}')[0],
    updated=(SELECT RAW FLOOR(NOW_MILLIS()/1000))[0]
    WHERE type='MD'
        AND docType='matsGui'
        AND subset='COMMON'
        AND version='V01'
        AND app='cb-surface'
        AND META().id='MD:matsGui:cb-surface:${model}:COMMON:V01'
%EODupdatemetadata
    )

    echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=${cmd}\""
    curl -s -u ${cred} http://${cb_host}:8093/query/service -d "statement=${cmd}"
    echo "---------------"
done
