#!/usr/bin/env bash

function is_epoch_rational {
    if [ -z "$1" ]; then
        echo "ERROR: no start epoch specified"
        usage
    fi

    if [ $1 -lt $(($(date +%s)-3600*24)) ]; then
        echo "ERROR: irrational epoch - prior to yesterday at this time"
        usage
    fi

    if [ $1 -gt $(date +%s) ]; then
        echo "ERROR: irrational epoch - beyond current time"
        usage
    fi
return 0
}

function derive_pattern_from_ids {
    # Determine an sql string pattern using wildcards that will be usable
    # in an sql query with a like statement.
    # Given a list of similar ids (same number of fields) this routine will
    # find the fields (':'seperated) that are not common
    # throught the list and substitute those fields with a "%25 (special char for %)" and return
    # that pattern.
    ids=$1
    # get the number of columns in these ingest_ids - just use the first one as they should all be the same
    num_columns=$(echo ${ids[0]} | awk -F ":" '{print NF}')
    differing_columns=()
    # find the columns that do not match for all the ingest ids
    for i in $(seq 1 $num_columns)
        do
            if [[ $(printf "%s\n" "${ids[@]}" | awk -F":" -vvar=$i '{print $var}' | awk '{$1=$1};1' | sort | uniq | wc -l) -ne 1 ]]
            then
                differing_columns+=( $i )
            fi
        done
    # now that we have the differing columns use the first id and replace the differing_columns with "%25 (special char for %)"
    pattern=${ids[0]}
    for i in $differing_columns
    do
        pattern=$(echo $pattern | awk -F ":" -vfield=5 'BEGIN { OFS=":" }{$field="%25"; print}')
    done
    echo ${pattern} | sed 's/:[&*].*$/:%25/g'
}

function get_id_pattern_from_load_spec {
    # Given a load spec this routine will get the metadata ids from the load spec,
    # if there are multiple type MD ids it will derive a common pattern for those
    # metadata ids and substitute the differing fields with "%25 (special char for %)
    # so that the common pattern can be used in an SQL++ query with a like statement.

    load_spec_file=$1
    # get the ingest documents from the load_spec - they are keyed with either "ingest_document_id"
    # for singular ones or "ingest_document_ids" for pural ones.
    # get the id field from the load_spec - these are ingest documents.
    # the ingest_ids for the id_field might be plural or singular (awk '{$1=$1};1' strips white space)
    id_field=$(grep ingest_document $load_spec_file | awk -F":" '{print $1}' | awk '{$1=$1};1')
    md_ids=()
    if [[ ${id_field} == "ingest_document_ids" ]];then 
        IFS=', ' read -r -a md_ids <<< $(${HOME}/VXingest/scripts/VXingest_utilities/yq -r ".load_spec.${id_field}[]" ${load_spec_file})
    else
        IFS=', ' read -r -a md_ids <<< $(${HOME}/VXingest/scripts/VXingest_utilities/yq -r ".load_spec.${id_field}" ${load_spec_file})
    fi
    # if there are no md_ids just leave
    if [[ ${#md_ids[@]} -eq 0 ]]; then
        echo "ERROR: no ingest document ids to process"
        exit 1
    fi

    # if there is only one md_ids use that, no need for an id pattern
    if [[ ${#md_ids[@]} -eq 1 ]]; then
        md_pattern="${md_ids[0]}"
    else
        md_pattern=$(derive_pattern_from_ids ${md_ids})
    fi
    #now get the DD document template ids for the pattern of all of the ingest documents from the load_spec
    # NOTE: curl URL's don't like '%' or ';' characters. replace them with '%25' and '%3B' respectively (you can leave the ';' at the end of the statement off, actually)
    template_ids=()
    IFS=', ' read -r -a template_ids <<< $(curl -s http://adb-cb1.gsd.esrl.noaa.gov:8093/query/service -u"${cred}" -d "statement=select raw template.id from mdata where meta().id like \"${md_pattern}\"" | jq -r '.results | .[]' | sed 's/:[&*].*$/:%25/g')
    template_pattern=$(derive_pattern_from_ids ${template_ids})
    echo ${template_pattern}
}

function get_metric_name_from_pattern {
    pattern=$1
    hostname=$(hostname -s)
    metric_name=$(echo ${pattern} | sed 's/:/_/g' | sed 's/%25/_wc_/g')
    # add hostname
    metric_name="${metric_name}_${hostname}"
    echo ${metric_name}
}

function get_record_count_from_log(){
    log_file=$1
    num_docs=0
    num_docs=$(grep "write_document_to_files writing .* documents" $log_file | awk '{Total=Total+$4} END{print Total}')
    echo $num_docs
}

function usage {
  echo "Usage $0 -c credentials-file -s start_epoch -f finish_epoch -l log_file -L load_spec -t textfile directory -e exit code"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "The start and finish epochs should be in seconds"
  echo "Thee metric name is the name used in the generated metrics as well as the prefix of the file name."
  echo "Metrics will be written into the textfile directory (-t)"
  echo "The load spec should be the load_spec file with its full path that was used for the ingest process"
  echo "The scrape_metrics.sh script scans the log for the intended_record_count, and any errors,"
  echo "and queries the database to determine how many records were just added."
  exit 1
}

while getopts 'c:s:f:l:L:t:e:' param; do
  case "${param}" in
  c)
    credentials_file=${OPTARG}
    if [[ ! -f "${credentials_file}" ]]; then
      echo "${credentials_file} does not exist"
      usage
    fi
    cb_host=$(grep cb_host ${credentials_file} | awk '{print $2}')
    cb_user=$(grep cb_user ${credentials_file} | awk '{print $2}')
    cb_pwd=$(grep cb_password ${credentials_file} | awk '{print $2}')
    cred="${cb_user}:${cb_pwd}"
    ;;
  s)
    start_epoch=${OPTARG}
    ;;
  f)
    finish_epoch=${OPTARG}
    ;;
  l)
    log_file=${OPTARG}
    if [[ ! -f "${log_file}" ]]; then
      echo "ERROR: log file ${log_file} does not exist"
      usage
    fi
    ;;
  L)
    load_spec=${OPTARG}
    if [[ ! -f "${load_spec}" ]]; then
      echo "ERROR: load spec file ${load_spec} does not exist"
      usage
    fi
    ;;
  t)
    textfile_dir=${OPTARG}
    if [[ ! -d "${textfile_dir}" ]]; then
      echo "ERROR: text file directory ${textfile_dir} does not exist"
      usage
    fi
    ;;
  e)
    exit_code=${OPTARG}
    ;;
  *)
    echo "ERROR: wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done

if [ -z "${start_epoch}" ]; then
    is_epoch_rational ${start_epoch}
fi

if [ -z "${finish_epoch}" ]; then
    is_epoch_rational ${finish_epoch}
fi

if [ -z "${textfile_dir}" ]; then
    echo "ERROR: no textfile_dir specified"
    usage
fi

#Get the error count from the g file
error_count=$(grep -i error ${log_file} | wc -l)

# Get the meta().id pattern that can be used to query
# for the metadata.cas fieds that have been changed
# between the start_epoch and the finish_epoch
document_id_pattern=$(get_id_pattern_from_load_spec ${load_spec})

# Get the metric name from the pattern
metric_name=$(get_metric_name_from_pattern ${document_id_pattern})

expected_duration_seconds=0
actual_duration_seconds=$((stop_epoch-finish_epoch))
error_count=${error_count}
intended_record_count=$(get_record_count_from_log "${log_file}")
# NOTE: curl URL's don't like '%' or ';' characters. replace them with '%25' and '%3B' respectively (you can leave the ';' at the end of the statement off, actually)
set -x
recorded_record_count=$(curl -s http://adb-cb1.gsd.esrl.noaa.gov:8093/query/service -u"${cred}" -d "statement=select count(meta().id) from mdata where CEIL(meta().cas / 1000000000) BETWEEN ${start_epoch} AND ${finish_epoch} AND meta().id like \"${document_id_pattern}\"" | jq -r '.results | .[]')
set +x
tmp_metric_file=/tmp/${metric_name}_$$
metric_file=${textfile_dir}/${metric_name}
echo "${metric_name}" > ${tmp_metric_file}
echo "{"  >> ${tmp_metric_file}
echo "log_file=${log_file}," >> ${tmp_metric_file}
echo "start_epoch=${start_epoch}," >> ${tmp_metric_file}
echo "stop_epoch=${finish_epoch}," >> ${tmp_metric_file}
echo "expected_duration_seconds=0," >> ${tmp_metric_file}
echo "actual_duration_seconds=${actual_duration_seconds}," >> ${tmp_metric_file}
echo "error_count=${error_count}," >> ${tmp_metric_file}
echo "intended_record_count=${intended_record_count}," >> ${tmp_metric_file}
echo "recorded_record_count=${recorded_record_count}," >> ${tmp_metric_file}
echo "exit_code=${exit_code}" >> ${tmp_metric_file}
echo "}" >> ${tmp_metric_file}
mv ${tmp_metric_file} ${metric_file}