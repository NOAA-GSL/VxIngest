#!/usr/bin/env bash

# Read the active job spec documents and determine which ones to run, and run them.
# Job spec documents have a schedule field as well as a run_priority.
# This script is expected to run on quarter hour intervals
# and select the job documents that are scheduled for this
# run-quarter, and then use the run_priority and to schedule the
# the jobs. If the offset minutes is specified then the job will
# be scheduled with 'at' for running at offset minutes from when this script is run.
# For all the jobs where the offset minutes is zero the jobs will be run in sequence
# in priority order. Each waiting for the previous to finish.
# This script expects to execute inside the clone directory of the VxIngest repo.
# This script expects to be run as user amb-verif.
# This script expects to have a python virtual environment in the amb-verif home directory in the subdirectory vxingest-env.
# This script will collect metrics about its own running, how many jobs run, what their job spec ids are,
# and this scripts run duration in seconds.

function usage {
  echo "Usage $0 -c credentials-file -d VxIngest directory -l log directory -o output directory -m metrics directory -x transfer directory [-j job_id -s start_epoch -e end_epoch]"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "The VxIngest directory specifies the directory where The VxIngest repo has been cloned."
  echo "The log directory is where the program will put its log files."
  echo "The output directory is where the ingest will write its result documents."
  echo "The transfer directory is where this script will put a tarball of the output data for importing."
  echo "The metrics directory is where the scraper will place the metrics."
  echo "The jobid is optional and if provided will run only the particular job, independent of the schedule of the job document as long as the type is 'JOB' or 'JOB-TEST' and status contains 'active'."
  exit 1
}

credentials_file=""
jobid=""
start_epoch=""
end_epoch=""
success_job_count=0
failed_job_count=0
start=$(date +%s)
clonedir=${PWD}
while getopts 'c:l:m:o:x:j:s:e:' param; do
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
  x)
    # remove the last '/' if it is there
    xfer_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${xfer_dir}" ]]; then
      echo "ERROR: Transfer directory ${xfer_dir} does not exist"
      usage
    fi
    ;;
  j)
    jobid=$(echo "${OPTARG}")
    ;;
  s)
    start_epoch="-f ${OPTARG}"
    ;;
  e)
    end_epoch="-l ${OPTARG}"
    ;;
  *)
    echo "ERROR: wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done

if [[ -z ${credentials_file} ]] || [[ -z ${log_dir} ]] || [[ -z ${metrics_dir} ]] || [[ -z ${output_dir} ]] || [[ -z ${xfer_dir} ]]; then
  echo "*missing parameter*"
  echo "provided credentials_file is ${credentials_file}"
  echo "provided log_dir is ${log_dir}"
  echo "provided metrics_dir is ${metrics_dir}"
  echo "provided output_dir is ${output_dir}"
  echo "provided xfer_dir is ${xfer_dir}"
  usage
fi

# read the active jobs and determine which ones to run, and run them.
# This script is expected to run in quarter hour intervals
# so we get the current qurter hour from date and use that to
# to select the job documents that are scheduled for this
# run quarter hour, run hour, etc. and then use at to schedule the
# the job for running at offset minutes from when this script is running

# what run hour and minute we are in.
current_hour=$(date +"%H")   # avoid divide by 0
current_minute=$(date +"%M") # avoid divide by 0
current_quarter=$(($current_minute / 15))

if [ ! -z ${jobid+x} ]; then
  echo "jobid is set to ${jobid}"
  read -r -d '' statement <<%EndOfJobStatement
SELECT meta().id AS id,
       LOWER(META().id) as name,
       run_priority,
       offset_minutes,
       LOWER(subType) as sub_type,
       input_data_path as input_data_path
FROM vxdata._default.METAR
WHERE id="${jobid}"
    AND (type = "JOB-TEST" or type = "JOB")
    AND version = "V01"
    AND CONTAINS(status, "active")
%EndOfJobStatement
else
  read -r -d '' statement <<%EndOfStatement
SELECT meta().id AS id,
       LOWER(META().id) as name,
       run_priority,
       offset_minutes,
       LOWER(subType) as sub_type,
       input_data_path as input_data_path
FROM vxdata._default.METAR
LET millis = ROUND(CLOCK_MILLIS()),
    sched = SPLIT(schedule,' '),
    minute = CASE WHEN sched[0] = '*' THEN DATE_PART_MILLIS(millis, 'minute', 'UTC') ELSE TO_NUMBER(sched[0]) END,
    hour = CASE WHEN sched[1] = '*' THEN DATE_PART_MILLIS(millis, 'hour', 'UTC') ELSE TO_NUMBER(sched[1]) END,
    day = CASE WHEN sched[2] = '*' THEN DATE_PART_MILLIS(millis, 'day', 'UTC') ELSE TO_NUMBER(sched[2]) END,
    month = CASE WHEN sched[3] = '*' THEN DATE_PART_MILLIS(millis, 'month', 'UTC') ELSE TO_NUMBER(sched[3]) END,
    year = CASE WHEN sched[4] = '*' THEN DATE_PART_MILLIS(millis, 'year', 'UTC') ELSE TO_NUMBER(sched[4]) END
WHERE type='JOB'
    AND version='V01'
    AND CONTAINS (status,'active')
    AND DATE_PART_MILLIS(millis, 'year', 'UTC') = year
    AND DATE_PART_MILLIS(millis, 'month', 'UTC') = month
    AND DATE_PART_MILLIS(millis, 'hour', 'UTC') = hour
    AND DATE_PART_MILLIS(millis, 'day', 'UTC') = day
    AND IDIV(DATE_PART_MILLIS(millis, 'minute', 'UTC'), 15) = IDIV(minute, 15)
ORDER BY offset_minutes,
         run_priority
%EndOfStatement
fi

# job_docs should either get one document if jobid was specified or all the currently scheduled job documents otherwise
job_docs=$(curl -s http://${cb_host}:8093/query/service -u"${cred}" -d "statement=${statement}" | jq -r '.results | .[]')
ids=($(echo $job_docs | jq -r .id))
if [ ! -z ${jobid+x} ]; then        # no jobid specified so got all the currently scheduled jobs
  if [[ ${#ids[@]} -eq 0 ]]; then #no jobs found currently scheduled
    echo "no jobs are currently scheduled for this time"
    exit 0
  fi
else
  if [[ ${#ids[@]} -eq 0 ]]; then #jobid specified but no job found
    echo "no jobs are currently scheduled for this time"
    exit 0
  fi
fi

names=($(echo $job_docs | jq -r .name))
offset_minutes=($(echo $job_docs | jq -r .offset_minutes))
run_priorities=($(echo $job_docs | jq -r .run_priority))
sub_types=($(echo $job_docs | jq -r .sub_type))
input_data_paths=($(echo $job_docs | jq -r .input_data_path))

runtime=$(date +\%Y-\%m-\%d:\%H:\%M:\%S)
hname=$(echo $(hostname -s) | tr '-' '_')
# if there was a valid jobid specified then we are only running one job
for i in "${!ids[@]}"; do
  echo "--------"
  echo "*************************************"
  job_id="${ids[$i]}"
  echo "*****${job_id}*****"
  # translate '_' to '__' and ':' to '_' for names
  name=$(echo "${names[$i]}" | sed 's/_/__/g' | sed 's/:/_/g')
  offset_minute="${offset_minutes[$i]}"
  run_priority="${run_priorities[$i]}"
  sub_type="${sub_types[$i]}"
  input_data_path="${input_data_paths[$i]}"

  # create the output dir from the sub_type i.e. /opt/data/netcdf_to_cb/output/
  # create subtype sub directory for code path and output path
  # make the output directory contain a timestamp
  # because if the output directory is timestamped we can run multiple jobs in parallel
  sub_dir="${sub_type}_to_cb"
  out_dir="${output_dir}/${sub_dir}/output/$(date +%Y%m%d%H%M%S)"
  mkdir -p $out_dir
  log_file="${log_dir}/${name}-${runtime}.log"

  # run the ingest job
  metric_name="${name}_${hname}"
  echo "metric_name ${metric_name}" >${log_file}
  # have to have a PYTHONPATH that includes the clonedir
  export PYTHONPATH=$(pwd)
  if [ -z ${jobid+x} ]; then # no jobid specified so got all the currently scheduled jobs
    threads=" -t8"
  else
    threads=""
  fi
  echo "RUNNING - python ${clonedir}/${sub_dir}/run_ingest_threads.py -j ${job_id} -c ${credentials_file} -o $out_dir ${threads}  ${start_epoch} ${end_epoch}"
  python ${clonedir}/${sub_dir}/run_ingest_threads.py -j ${job_id} -c ${credentials_file} -o $out_dir ${threads} ${start_epoch} ${end_epoch} >>${log_file} 2>&1
  exit_code=$?
  if [[ "${exit_code}" -ne "0" ]]; then
    failed_job_count=$((failed_job_count + 1))
  else
    success_job_count=$((success_job_count + 1))
  fi
  echo "exit_code:${exit_code}" >>${log_file}
  tar_file_name="${metric_name}_$(date +%s).tar.gz"

  # mv the log_file to the output dir
  mv ${log_file} ${out_dir}
  # tar the output dir into the transfer directory and remove the files
  tar -czf ${xfer_dir}${tar_file_name} --remove-files -C ${out_dir} .
  # rm the output directory if it is empty
  find ${outdir} -depth -type d -empty -exec rmdir {} \;
done
echo "FINISHED"
end=$(date +%s)
m_file=$(mktemp)
echo "run_ingest_duration $((end - start))" >${m_file}
echo "run_ingest_success_count ${success_job_count}" >>${m_file}
echo "run_ingest_failure_count ${failed_job_count}" >>${m_file}
mv ${m_file} "${metrics_dir}/run_ingest_metrics.prom"
exit 0
