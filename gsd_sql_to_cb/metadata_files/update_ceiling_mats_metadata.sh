#!/bin/sh
if [ $# -ne 1 ]; then
  echo "Usage $0 credentials-file"
  exit 1
fi
if [[ ! -f "$1" ]]; then
  echo "$1 is not a vslid file - exiting"
  exit 1
fi
credentials=$1

echo source the functions file
. ${HOME}/VXingest/scripts/VXingest_utilities/ingest_functions.sh
DO_CREDENTIALS "$credentials"

for model in HRRR HRRR_OPS RAP_OPS RRFS_dev1
  do
  cmd=$(cat <<-%EODupdatemetadata
    UPDATE mdata
    SET thresholds = (
        SELECT DISTINCT RAW d_thresholds
        FROM (
            SELECT OBJECT_NAMES(object_names_t.data) AS thresholds
            FROM mdata AS object_names_t
            WHERE object_names_t.type='DD'
                AND object_names_t.docType='CTC'
                AND object_names_t.subset='METAR'
                AND object_names_t.version='V01'
                AND object_names_t.model='${model}') AS d
        UNNEST d.thresholds AS d_thresholds),
    fcstLens=(
        SELECT RAW ARRAY_AGG(DISTINCT fl.fcstLen)
        FROM mdata AS fl
        WHERE fl.type='DD'
            AND fl.docType='CTC'
            AND fl.subset='METAR'
            AND fl.version='V01'
            AND fl.model='${model}'),
    regions=(
        SELECT RAW ARRAY_AGG(DISTINCT r.region)
        FROM mdata AS r
        WHERE r.type='DD'
            AND r.docType='CTC'
            AND r.subset='METAR'
            AND r.version='V01'
            AND r.model='${model}'),
    numrecs=(
        SELECT RAW COUNT(META().id)
        FROM mdata AS n
        WHERE n.type='DD'
            AND n.docType='CTC'
            AND n.subset='METAR'
            AND n.version='V01'
            AND n.model='${model}')[0],
    mindate=(
        SELECT RAW MIN(mt.fcstValidEpoch) AS mintime
        FROM mdata AS mt
        WHERE mt.type='DD'
            AND mt.docType='model'
            AND mt.subset='METAR'
            AND mt.version='V01'
            AND mt.model='${model}')[0],
    maxdate=(
        SELECT RAW MAX(mat.fcstValidEpoch) AS maxtime
        FROM mdata AS mat
        WHERE mat.type='DD'
            AND mat.docType='model'
            AND mat.subset='METAR'
            AND mat.version='V01'
            AND mat.model='${model}')[0],
    updated=(SELECT RAW FLOOR(NOW_MILLIS()/1000))[0]
    WHERE type='MD'
        AND docType='matsGui'
        AND subset='COMMON'
        AND version='V01'
        AND META().id='MD:matsGui:cb-ceiling:${model}:COMMON:V01';
%EODupdatemetadata
)

  echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=${cmd}\""
  curl -s -u ${cred} http://${cb_host}:8093/query/service -d "statement=${cmd}"
  echo "---------------"
done