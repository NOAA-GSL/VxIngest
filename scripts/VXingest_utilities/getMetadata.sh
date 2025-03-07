#!/bin/bash

function usage {
  echo "Usage $0 -c credentials-file [-d VxIngest_directory -o output_file]"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "The optional VxIngest_directory specifies the directory where The VxIngest repo has been cloned. if this is specified the data will be written to"
  echo "${VxIngest_directory}/mats_metadata_and_indexes/metadata_files/meta_data.json."
  echo "The optional output_file overrides the VxIngest directory and will cause the metadata to be written to ${output_file}."
  echo "If neither the VxIngest_directory or the output_file are specified the data will be written to standard output."
  echo "This script expects to execute inside the clone directory of the VxIngest repo"
  echo "This script expects the couchbase cbq utility to be in the path."
  exit 1
}
unset out
unset output_file
unset clonedir
gitroot=$(git rev-parse --show-toplevel)
if [ "$gitroot" != "$(pwd)" ]; then
  echo "$(pwd) is not a git root directory:"
  usage
fi

while getopts 'c:d:o:' param; do
  case "${param}" in
  c)
    credentials_file=${OPTARG}
    if [[ ! -f "${credentials_file}" ]]; then
      echo "${credentials_file} does not exist"
      usage
    fi
    cb_host=$(grep cb_host ${credentials_file} | awk '{print $2}')
    # if it is a multinode host split on ',' and take the first one
    IFS=','
    read -ra hostarr <<<"$cb_host"
    cb_host=${hostarr[0]}
    cb_user=$(grep cb_user ${credentials_file} | awk '{print $2}')
    cb_pwd=$(grep cb_password ${credentials_file} | awk '{print $2}')
    bucket=$(grep cb_bucket ${credentials_file} | awk '{print $2}')
    collection=$(grep cb_collection ${credentials_file} | awk '{print $2}')
    scope=$(grep cb_scope ${credentials_file} | awk '{print $2}')
    cred="${cb_user}:${cb_pwd}"
    ;;
  d)
    # remove the last '/' if it is there
    clonedir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${clonedir}" ]]; then
      echo "ERROR: VxIngest clone directory ${clonedir} does not exist"
      usage
    fi
    ;;
  o)
    output_file=$(echo "${OPTARG}" | sed 's|/$||')
    ;;
  *)
    echo "ERROR: wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done
if [[ "X${output_file}" == "X" ]]; then
  if [[ "X${clonedir}" == "X" ]]; then
    out=""
  else
    out="${clonedir}/mats_metadata_and_indexes/metadata_files/metadata.json"
  fi
else
  out="${output_file}"
fi

if [[ "X${out}" == "X" ]]; then
  cbq -no-ssl-verify -e "${cb_host}" -u "${cb_user}" -p "${cb_pwd}" -quiet -no-ssl-verify -s="select ${collection}.* from ${bucket}._default.${collection} where type = 'MD'" | grep -v 'Disabling' | jq --unbuffered '.results'
else
  cbq -no-ssl-verify -e "${cb_host}" -u "${cb_user}" -p "${cb_pwd}" -quiet -no-ssl-verify -s="select ${collection}.* from ${bucket}._default.${collection} where type = 'MD'" | grep -v 'Disabling' | jq --unbuffered '.results' >${out}
fi
