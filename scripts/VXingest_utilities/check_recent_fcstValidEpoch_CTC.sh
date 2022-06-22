#!/usr/bin/env sh
function usage() {
  echo "Usage $0 -c credentials-file -s subset, -m model -h hours"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "This script assumes that you have cloned VXingest into ${HOME}/VXingest"
  echo "If you cloned it elsewhere, make a link."
  echo "returns the latest fcstValidEpoch for the corresponding CEILING CTC document for the given model that exists within the last h hours"
  exit 1
}
while getopts 'c:s:m:h:' param; do
  case "${param}" in
  c)
    credentials_file=${OPTARG}
    if [ ! -f "${credentials_file}" ]; then
      echo "${credentials_file} does not exist"
      usage
    fi
    ;;
  s)
    subset=${OPTARG}
    ;;
  m)
    model=${OPTARG}
    ;;
  h)
    hours=${OPTARG}
    ;;
  *)
    echo "wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done

if [ ! -f "${credentials_file}" ]; then
  echo "no credentials_file specified"
  usage
fi

host=$(grep cb_host ${credentials_file} | awk '{print $2}')
user=$(grep cb_user ${credentials_file} | awk '{print $2}')
pwd=$(grep cb_password ${credentials_file} | awk '{print $2}')
if [ -z "${host}" ]; then
  echo "credentials do not specify cb_host"
  usage
fi
if [ -z "${user}" ]; then
  echo "credentials do not specify cb_user"
  usage
fi
if [ -z "${pwd}" ]; then
  echo "credentials do not specify cb_password"
  usage
fi

recently_added=$(curl -s -u "${user}:${pwd}" http://${host}:8093/query/service  -d "statement=SELECT COUNT(fcstValidEpoch) as recently_added FROM mdata WHERE type='DD' AND version='V01' AND subset='${subset}' AND model='${model}' AND docType='CTC' AND subDocType='CEILING' AND fcstValidEpoch > (CLOCK_MILLIS() / 1000) - 3600 * ${hours};" | jq -r '.results | .[] | .recently_added')
if [[ -z $recently_added ]]; then
  echo "No recent CEILING CTC documents found for subset ${subset} and model ${model} within the last ${hours} hours."
  exit 1
else
  echo $recently_added
  exit 0
fi

