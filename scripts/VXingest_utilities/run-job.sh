#!/usr/bin/env sh

# read the active jobs and determine which ones to run, and run them.
# Job documents have an interval_minutes and an offset_minutes field as well as a run_priority.
# This script is expected to run on quarter hour intervals
# and determine the current qurter hour from "date" and use that to
# to select the job documents that are scheduled for this
# run quarter hour and run hour, and then use the run_priority and "at" to schedule the
# the job for running at offset minutes from when this script is run.
# This script expects to execute inside the clone directory of the VxIngest repo.
# This script expects to be run as user amb-verif.
# This script expects to have a python virtual environment in the amb-verif home directory in the subdirectory vxingest-env.

function usage {
  echo "Usage $0 -c credentials-file -d VxIngest directory -l log directory -o output_dir -m metrics_directory"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "The VxIngest directory specifies the directory where The VxIngest repo has been cloned."
  echo "The log directory is where the program will put its log files"
  echo "The output_dir is where the ingest will write its result documents"
  echo "The metrics directory is where the scraper will place the metrics"
  echo "This script expects to execute inside the clone directory of the VxIngest repo"
  echo "This script expects to be run as user amb-verif"
  echo "This script expects to have a python virtual environment in the amb-verif home directory in the subdirectory vxingest-env"
  exit 1
}

while getopts 'c:d:l:m:o:' param; do
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
  d)
    # remove the last '/' if it is there
    clonedir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${clonedir}" ]]; then
      echo "ERROR: VxIngest clone directory ${clonedir} does not exist"
      usage
    fi
    ;;
  l)
    # remove the last '/' if it is there
    log_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${log_dir}" ]]; then
      echo "ERROR: VxIngest log directory ${log_dir} does not exist"
      usage
    fi
    ;;
  m)
    # remove the last '/' if it is there
    metrics_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${metrics_dir}" ]]; then
      echo "ERROR: VxIngest metrics directory ${metrics_dir} does not exist"
      usage
    fi
    ;;
  o)
    # remove the last '/' if it is there
    output_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${output_dir}" ]]; then
      echo "ERROR: VxIngest input directory ${output_dir} does not exist"
      usage
    fi
    ;;
  *)
    echo "ERROR: wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done

pid=$$
if [ "$(whoami)" != "amb-verif" ]; then
        echo "Script must be run as user: amb-verif"
        exit 255
fi

source ${HOME}/vxingest-env/bin/activate

cd ${clonedir} && export PYTHONPATH=`pwd`
gitroot=$(git rev-parse --show-toplevel)
if [ "$gitroot" != "$(pwd)" ];then
        echo "$(pwd) is not a git root directory: Usage $0 VxIngest_clonedir"
        exit 1
fi

# read the active jobs and determine which ones to run, and run them.
# This script is expected to run in quarter hour intervals
# so we get the current qurter hour from date and use that to
# to select the job documents that are scheduled for this
# run quarter hour, run hour, etc. and then use at to schedule the
# the job for running at offset minutes from when this script is running

# what run hour and minute we are in.
current_hour=$(date +"%H")  # avoid divide by 0
current_minute=$(date +"%M")   # avoid divide by 0
current_quarter=$(($current_minute / 15))

read -r -d '' statement <<- %EndOfStatement
SELECT META().id AS id,
       LOWER(META().id) as name,
       run_priority,
       offset_minutes,
       LOWER(subType) as sub_type,
       input_data_path as input_data_path
FROM mdata
LET millis = ROUND(CLOCK_MILLIS()),
    sched = SPLIT(schedule,' '),
    minute = CASE WHEN sched[0] = '*' THEN DATE_PART_MILLIS(millis, 'minute', 'UTC') ELSE TO_NUMBER(sched[0]) END,
    hour = CASE WHEN sched[1] = '*' THEN DATE_PART_MILLIS(millis, 'hour', 'UTC') ELSE TO_NUMBER(sched[1]) END,
    day = CASE WHEN sched[2] = '*' THEN DATE_PART_MILLIS(millis, 'day', 'UTC') ELSE TO_NUMBER(sched[2]) END,
    month = CASE WHEN sched[3] = '*' THEN DATE_PART_MILLIS(millis, 'month', 'UTC') ELSE TO_NUMBER(sched[3]) END,
    year = CASE WHEN sched[4] = '*' THEN DATE_PART_MILLIS(millis, 'year', 'UTC') ELSE TO_NUMBER(sched[4]) END
WHERE type='JOB'
    AND version='V01'
    AND status='active'
    AND DATE_PART_MILLIS(millis, 'year', 'UTC') = year
    AND DATE_PART_MILLIS(millis, 'month', 'UTC') = month
    AND DATE_PART_MILLIS(millis, 'hour', 'UTC') = hour
    AND DATE_PART_MILLIS(millis, 'day', 'UTC') = day
    AND IDIV(DATE_PART_MILLIS(millis, 'minute', 'UTC'), 15) = IDIV(minute, 15)
ORDER BY offset_minutes,
         run_priority
%EndOfStatement

job_docs=$(curl -s http://adb-cb1.gsd.esrl.noaa.gov:8093/query/service -u"${cred}" -d "statement=${statement}" | jq -r '.results | .[]')
ids=($(echo $job_docs | jq -r .id))
names=($(echo $job_docs | jq -r .name))
offset_minutes=($(echo $job_docs | jq -r .offset_minutes))
run_priorities=($(echo $job_docs | jq -r .run_priority))
sub_types=($(echo $job_docs | jq -r .sub_type))
input_data_paths=($(echo $job_docs | jq -r .input_data_path))
if [[ ${#ids[@]} -eq 0 ]]; then
    echo "no jobs are currently scheduled for this time"
    exit 0
fi

if [ ! -d "${HOME}/logs" ]; then
        mkdir ${HOME}/logs
fi

runtime=`date +\%Y-\%m-\%d:\%H:\%M:\%S`
hname=$(echo $(hostname -s) | tr '-' '_')

for i in "${!ids[@]}"; do
  echo "--------"
  echo "*************************************"
  job_id="${ids[$i]}"
  echo "*****${job_id}*****"
  # translate '_' to '__' and ':' to '_' for names
  name=$(echo "${ids[$i]}" | sed 's/_/__/g' | sed 's/:/_/g')
  offset_minute="${offset_minutes[$i]}"
  run_priority = "${run_priorities[$i]}"
  sub_type="${sub_types[$i]}"
  input_data_path="${input_data_paths[$i]}"

  # create the output dir from the sub_type i.e. /data/netcdf_to_cb/output/
  # create subtype sub directory for code path and output path
  sub_dir="${sub_type}_to_cb"
  outdir="${output_dir}/${sub_dir}/output/${pid}"
  mkdir -p $outdir
  log_file="${log_dir}/${name}-${runtime}.log"
  import_log_file="${log_dir}/import-${name}-${runtime}.log"

  # run the ingest job
  metric_name="${name}_${hname}"
  echo "${metric_name}" > ${log_file}
  # provide an input_data_path if there was one in the job spec (netcdf and grib)
  input_data_path_param=""
  if [[ "${input_data_paths[3]}" != "null" ]]; then
     input_data_path_param="-p ${input_data_path}"
  fi
  python ${clonedir}/${sub_dir}/run_ingest_threads.py -j ${jid} -c ~/adb-cb1-credentials ${input_data_path_param} -o $outdir -t8 >> ${log_file} 2>&1
  exit_code=$?
  echo "exit_code:${exit_code}" >> ${log_file}

  # run the import job
  metric_name="import_${name}_${hname}"
  echo ${metric_name} > ${import_log_file}
  ${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p ${outdir} -n 8 -l ${clonedir}/logs >> ${import_log_file} 2>&1
  exit_code=$?
  echo "exit_code:${exit_code}" >> ${import_log_file}

  # run the scraper
  sleep 2
  ${clonedir}/scripts/VXingest_utilities/scrape_metrics.sh -c ~/adb-cb1-credentials -l ${log_file} -d ${metrics_dir}
  echo "--------"
done

echo "*************************************"
echo "update metadata"
${clonedir}/mats_metadata_and_indexes/metadata_files/update_ceiling_mats_metadata.sh ~/adb-cb1-credentials
# eventually we need to scrape the update....
echo "FINISHED"
date
