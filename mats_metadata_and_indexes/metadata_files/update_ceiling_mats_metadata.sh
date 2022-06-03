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
models_requiring_metadata=($(curl -s -u ${cred} http://${cb_host}:8093/query/service -d statement='SELECT DISTINCT RAW (SPLIT(meta(mdata).id,":")[3]) model FROM mdata WHERE type="DD" AND docType="CTC" AND subDocType="CEILING" AND version="V01";' | jq -r '.results[]'))
echo "------models_requiring metadata--${models_requiring_metadata[@]}"
#get models having metadata but no data (remove metadata for these)
#(note 'like %' is changed to 'like %25')
remove_metadata_for_models=($(curl -s -u ${cred} http://${cb_host}:8093/query/service -d statement='SELECT raw m FROM (SELECT RAW SPLIT(META().id,":")[3] AS model FROM mdata WHERE META().id LIKE "MD:matsGui:cb-ceiling:%25:COMMON:V01" AND type="MD" AND docType="matsGui" AND version="V01" ORDER BY model) AS m WHERE m not IN (select distinct raw model from mdata where type="DD" and docType="CTC" and subDocType="CEILING" and version="V01" order by model);' | jq -r '.results[]'))
echo "------models not requiring metadata (remove metadata)--${remove_metadata_for_models[@]}" # process models
# remove metadata for models with no data
for model in ${remove_metadata_for_models[@]}; do
     cmd="delete FROM mdata WHERE type='MD' AND docType='matsGui' AND subset='COMMON' AND version='V01' AND app='cb-ceiling' AND META().id='MD:matsGui:cb-ceiling:${model}:COMMON:V01'"
     echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=${cmd}\""
     curl -s -u ${cred} http://${cb_host}:8093/query/service -d "statement=${cmd}"
done
# initialize the metadata for the models for which the metadata does not exist
models_with_existing_metadata=($(curl -s -u ${cred} http://${cb_host}:8093/query/service -d statement='select raw SPLIT(meta().id,":")[3]  FROM mdata WHERE type="MD" AND docType="matsGui" AND subset="COMMON" AND version="V01" AND app="cb-ceiling" AND META().id LIKE "MD:matsGui:cb-ceiling:%25:COMMON:V01";' | jq -r '.results[]'))
for m in ${models_requiring_metadata[@]}; do
    if [[ ! " ${models_with_existing_metadata[@]} " =~ " ${m} " ]]; then
        # initialize the metadata for this model - it will get updated in the next step
        echo "initializing metadata for model $m"
        cmd=$(
            cat <<-%EODinsertmetadata
            INSERT INTO mdata (KEY, VALUE)
            VALUES ("MD:matsGui:cb-ceiling:${m}:COMMON:V01",
              {"id": "MD:matsGui:cb-ceiling:${m}:COMMON:V01",
                "type": "MD",
                "docType": "matsGui",
                "app": "cb-ceiling",
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
                "fcstLens": [],
                "thresholds": []})
%EODinsertmetadata
        )
        echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=${cmd}\""
        curl -s -u ${cred} http://${cb_host}:8093/query/service -d "statement=${cmd}"
    fi
done

# now update all the metdata for all the models that require it
for model in "${models_requiring_metadata[@]}"; do
    cmd=$(
        cat <<-%EODupdatemetadata
    UPDATE mdata
    SET thresholds = (
        SELECT DISTINCT RAW d_thresholds
        FROM (
            SELECT OBJECT_NAMES(object_names_t.data) AS thresholds
            FROM mdata AS object_names_t
            WHERE object_names_t.type='DD'
                AND object_names_t.docType='CTC'
                AND object_names_t.version='V01'
                AND object_names_t.model='${model}') AS d
        UNNEST d.thresholds AS d_thresholds),
    fcstLens=(
    SELECT DISTINCT VALUE fl.fcstLen
    FROM mdata as fl
    WHERE fl.type='DD'
        AND fl.docType='CTC'
        AND fl.version='V01'
        AND fl.model='${model}'
        ORDER BY fl.fcstLen),
    regions=(
    SELECT DISTINCT VALUE rg.region
    FROM mdata as rg
    WHERE rg.type='DD'
        AND rg.docType='CTC'
        AND rg.version='V01'
        AND rg.model='${model}'
    ORDER BY r.mdata.region),
    displayText=(SELECT RAW m.standardizedModelList.${model}
        FROM mdata AS m
        USE KEYS "MD:matsAux:COMMON:V01")[0],
    displayCategory=(select raw 1)[0],
    displayOrder=(
        WITH k AS
            ( SELECT RAW m.standardizedModelList.${model}
            FROM mdata AS m
            USE KEYS "MD:matsAux:COMMON:V01" )
        SELECT RAW m.primaryModelOrders.[k[0]].m_order
        FROM mdata AS m
        USE KEYS "MD:matsAux:COMMON:V01")[0],
    mindate=(
        SELECT RAW MIN(mt.fcstValidEpoch) AS mintime
        FROM mdata AS mt
        WHERE mt.type='DD'
            AND mt.docType='CTC'
            AND mt.version='V01'
            AND mt.model='${model}')[0],
    maxdate=(
        SELECT RAW MAX(mat.fcstValidEpoch) AS maxtime
        FROM mdata AS mat
        WHERE mat.type='DD'
            AND mat.docType='CTC'
            AND mat.version='V01'
            AND mat.model='${model}')[0],
    numrecs=(
        SELECT RAW COUNT(META().id)
        FROM mdata AS n
        WHERE n.type='DD'
            AND n.docType='CTC'
            AND n.version='V01'
            AND n.model='${model}')[0],
    updated=(SELECT RAW FLOOR(NOW_MILLIS()/1000))[0]
    WHERE type='MD'
        AND docType='matsGui'
        AND subset='COMMON'
        AND version='V01'
        AND app='cb-ceiling'
        AND META().id='MD:matsGui:cb-ceiling:${model}:COMMON:V01';
%EODupdatemetadata
    )

    echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=${cmd}\""
    curl -s -u ${cred} http://${cb_host}:8093/query/service -d "statement=${cmd}"
    echo "---------------"
done
