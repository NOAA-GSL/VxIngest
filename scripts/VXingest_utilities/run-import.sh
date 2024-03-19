#!/usr/bin/env bash
# Check the import directory to see if any new tarballs are there.
# If there are any, one by one untar them into their own temp directory
# and import the data documents, there should be data files and one log file in each,
# Creat an import log after the name of the associated log file (from the tarball).
# Scrape the logfiles and update the metrics.
# On success destroy each temporary directory and the associated tarball.
# On failure archive the tarball, and destroy the temp directory.
# This script expects to execute inside the VxIngest directory.
# This script expects to be run as user amb-verif very frequently (like two minutes).
# This script expects to have a python virtual environment in the amb-verif home directory in the subdirectory vxingest-env.
# This script will generate metrics to track its success or failure, and to track how frequently it runs.

# NOTE! nproc is not available on MACOS
# To run this script you must make an alias for nproc
# alias nproc="sysctl -n hw.ncpu"
# flock is not available on MACOS
# To run this script on MACOS you must brew install flock

# set the timezone to UTC0 so that the date command will return the correct epoch time
export TZ="UTC0"


function usage {
  echo "Usage $0 -c credentials-file -l load directory -t temp_dir -m metrics_directory [-n]"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "The load directory is where the program will look for the tar files"
  echo "The temp_dir directory is where the program will unbundle the tar files (in uniq temporary subdirs)"
  echo "The metrics directory is where the scraper will place the metrics"
  echo "the optional parameter -n will prevent running the metrics scraper (used for re-running import archives)"
  echo "This script expects to execute inside the VxIngest directory"
  echo "This script expects to be run as user amb-verif"
  failed_import_count=$((failed_import_count + 1))
  exit 1
}
success_import_count=0
failed_import_count=0
success_scrape_count=0
failed_scrape_count=0
scrape="true"

function import_archive {
  # import the data
  # the data is in the temp directory
  # the log file is in the temp directory
  # the log file is named after the tarball
  # the log file is
  f=$1
  archive_dir=$2
  temp_dir=$3
  scrape=$4
  start_epoch=$(date +%s)
  t_dir=$(mktemp -d --tmpdir=${temp_dir})
  if [[ ! -d "${t_dir}" ]]; then
    echo "ERROR: Failed to create VxIngest temp directory ${t_dir}"
    usage
  fi
  # get the subdir from the tarball - all the archives are in a subdir in the tarball
  subdir=$(tar -tzf ${f} | awk -F'/' '{print $1}' | uniq)
  echo "processing the tar file ${f}"
  data_dir="${t_dir}/${subdir}"
  echo "extracting tarball ${f} to temp_dir ${t_dir}"
  echo "tar -xzf ${f} -C ${t_dir}"
  # NOTE: the archives are tar'd into a subdirectory so data_dir is t_dir/sub_dir
  tar -xzf "${f}" -C "${t_dir}"
  if [[ $? != 0 ]]; then
    echo "ERROR: tarball ${f} failed to extract"
    base_f=$(basename $f)
    echo "moving tar file ${f} to ${archive_dir}/failed-extract-${base_f}"
    failed_import_count=$((failed_import_count + 1))
    # doing cp then rm because of an issue with docker mounts on MAC
    echo cp $f "${archive_dir}/failed-extract-${base_f}"
    mv $f "${archive_dir}/failed-extract-${base_f}"
    rm -rf ${t_dir}
    failed_import_count=$((failed_import_count + 1))
    return # go to the next tar file
  fi
  echo "finished extracting tarball ${f} to ${t_dir}"
  log_file_count=$(ls -1 ${data_dir}/*.log | wc -l)
  if [[ ${log_file_count} -ne 1 ]]; then
    echo "There is not just one log_file in ${data_dir} - extracted from ${f} - there are ${log_file_count}"
    base_f=$(basename $f)
    echo "moving tar file ${f} to ${archive_dir}/failred-too-many-log-files-${base_f}"
    echo " - exiting"
    # doing cp then rm because of an issue with docker mounts on MAC
    echo cp $f "${archive_dir}/failed-too-many-log-files-${base_f}"
    mv $f "${archive_dir}/failed-too-many-log-files-${base_f}"
    rm -rf ${t_dir}
    return # go to the next tar file
  fi
  # ok - have one log file
  log_file=$(ls -1 ${data_dir}/*.log)
  echo "processing log_file ${log_file}"
  log_dir=$(dirname ${log_file})
  mkdir -p ${log_dir}
  # run the import job
  metric_name=$(grep metric_name ${log_file} | awk '{print $6}') # Grab the desired column from the python log format
  echo "metric name will be ${metric_name}"
  # NOTE! nproc is not available on MACOS
  # To run this script you must make an alias for nproc
  # alias nproc="sysctl -n hw.ncpu"
  number_of_cpus=$(nproc)

  for json_f in ${data_dir}/*.json; do
    fname=$(basename ${json_f})
    ${HOME}/cbtools/bin/cbimport json --threads ${number_of_cpus} --cluster ${cb_host} --bucket ${bucket} --scope-collection-exp ${scope}.${collection} --username ${cb_user} --password ${cb_pwd} --format list --generate-key %id% --dataset file:///${json_f}
  done
  if [[ "${exit_code}" -ne "0" ]]; then
    echo "import failed for $f exit_code:${exit_code}"
    failed_import_count=$((failed_import_count + 1))
    base_f=$(basename $f)
    echo "moving tar file ${f} to ${archive_dir}/failed-import-${base_f}"
    echo mv $f "${archive_dir}/failed-import-${base_f}"
    mv $f "${archive_dir}/failed-import-${base_f}"
    # don't return or remove the t_dir yet - let the scraper record the error
  else
    echo "import succeeded for $f success_import_count: ${success_import_count}"
    success_import_count=$((success_import_count + 1))
    # save the tar file
    base_f=$(basename $f)
    echo "moving tar file ${f} to ${archive_dir}/success-${base_f}"
    echo mv $f "${archive_dir}/success-${base_f}"
    mv $f "${archive_dir}/success-${base_f}"
    # don't return or remove the t_dir yet  - let the scraper record the error
  fi
  stop_epoch=$(date +%s)
  # run the scraper
  if [ scrape == "true" ]; then
    sleep 2 # eventually consistent data - give it a little time
    echo "RUNNING - scripts/VXingest_utilities/scrape_metrics.sh -c ${credentials_file} -b ${start_epoch} -e ${stop_epoch} -l ${log_file} -d ${metrics_dir}"
    scripts/VXingest_utilities/scrape_metrics.sh -c ${credentials_file} -b ${start_epoch} -e ${stop_epoch} -l ${log_file} -d ${metrics_dir}
    exit_code=$?
    if [[ "${exit_code}" -ne "0" ]]; then
      failed_scrape_count=$((failed_scrape_count + 1))
    else
      success_scrape_count=$((success_scrape_count + 1))
    fi
  fi
  echo "removing temp dir ${t_dir}/*"
  rm -rf ${t_dir}
  echo "--------"
} # end import_archive

# main
# do not allow more than 5 processes (10 by ps) to run simultaneously
# since they are started by cron, if a lot of reruns are moved into the xfer directory
# at once it is possible to get multiple cronjobs trying to process the files simultaneously.
# If there are already 5 jobs running, don't allow another to start.
running_jobs=$(ps -elf | grep run-import | grep -v grep | wc -l)
if [ $running_jobs -gt 10 ]; then
	echo "too many jobs running - refusing this one"
	exit 1
fi
 
start=$(date +%s)
while getopts 'c:l:t:m:n' param; do
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
  l)
    # remove the last '/' if it is there
    export load_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${load_dir}" ]]; then
      echo "ERROR: Work load directory ${load_dir} does not exist"
      usage
    fi
    ;;
  t)
    # remove the last '/' if it is there
    export temp_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${temp_dir}" ]]; then
      echo "ERROR: tar file directory ${temp_dir} does not exist"
      usage
    fi
    ;;
  m)
    # remove the last '/' if it is there
    export metrics_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${metrics_dir}" ]]; then
      echo "ERROR: VxIngest metrics directory ${metrics_dir} does not exist"
      usage
    fi
    ;;
  n)
    # do not run the scraper
    scrape="false"
    ;;
  *)
    echo "ERROR: wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done
if [[ -z ${credentials_file} ]] || [[ -z ${load_dir} ]] || [[ -z ${metrics_dir} ]] || [[ -z ${temp_dir} ]]; then
  echo "*missing parameter*"
  echo "provided credentials_file is ${credentials_file}"
  echo "provided load_dir is ${load_dir}"
  echo "provided metrics_dir is ${metrics_dir}"
  echo "provided temp_dir is ${temp_dir}"
  usage
fi

if [ "$(whoami)" != "amb-verif" ]; then
  echo "Script must be run as user: amb-verif"
  usage
fi

# Check the load directory for new tar balls.
# This script is expected to run in intervals
# create a temporary log_dir
# create an archive dir (might already exist)
# The load_dir is where the program will look for the tar files
# the t_dir is where the tarball will be untar'd
archive_dir="${load_dir}/archive"
mkdir -p "${archive_dir}"
if [[ ! -d "${archive_dir}" ]]; then
  echo "ERROR: VxIngest archive directory ${archive_dir} does not exist"
  usage
fi
if [ ! -w "${archive_dir}" ]; then
  echo "archive directory ${archive_dir} IS NOT WRITABLE"
  usage
fi

ls -1 ${load_dir}/*.gz | while read f; do
  # lock the archive
  if { set -C; 2>/dev/null >"${f}.lock"; }; then
    # set a trap in case of some unsexpected exit in import_archive
    trap "rm -f ${f}.lock" EXIT
  else
    echo "skipping ${f} - it is being processed"
    continue
  fi
  import_archive $f $archive_dir $temp_dir $scrape
  # unlock the archive (${f} should have already been archived)
  rm "${f}.lock"
done

# update metadata  - currently disabled
update_metadata_enabled="false"
if [ ${update_metadata_enabled} == "true" ] && [ "${success_import_count}" -ne "0" ]; then
  echo "update metadata import success count: ${success_import_count}"
  LOCKDIR="/data/import_lock"
  #if LOCKDIR is > 48 * 3600 seconds old, remove it
  if (($(date "+%s") - $(date -r ${LOCKDIR} "+%s") > $((48 * 3600)))); then
    echo "removing old lock file"
    rm -rf ${LOCKDIR}
  fi
  if mkdir -- "$LOCKDIR"; then
    echo "update ceiling metadata"
    mats_metadata_and_indexes/metadata_files/update_ctc_ceiling_mats_metadata.sh ${credentials_file}
    ret=$?
    if [[ "${ret}" -ne "0" ]]; then
      echo "ceiling metadata update failed with exit code ${ret}"
    fi
    echo "update ceiling metadata"
    mats_metadata_and_indexes/metadata_files/update_ctc_visibility_mats_metadata.sh ${credentials_file}
    ret=$?
    if [[ "${ret}" -ne "0" ]]; then
      echo "visibility import failed with exit code ${ret}"
    fi
    echo "update visibility metadata"
    mats_metadata_and_indexes/metadata_files/update_sums_surface_mats_metadata.sh ${credentials_file}
    ret=$?
    if [[ "${ret}" -ne "0" ]]; then
      echo "surface import failed with exit code ${ret}"
    fi
    echo "update surface metadata"
    if rmdir -- "$LOCKDIR"; then
      echo "import finished"
    else
      echo "IMPORT ERROR: Could not remove import lock dir" >&2
    fi
  fi
fi
end=$(date +%s)
m_file=$(mktemp)
echo "run_import_duration $((end - start))" >${m_file}
echo "run_import_success_count ${success_import_count}" >>${m_file}
echo "run_import_failure_count ${failed_import_count}" >>${m_file}
echo "run_scrape_success_count ${success_scrape_count}" >>${m_file}
echo "run_scrape_failure_count ${failed_scrape_count}" >>${m_file}
mv ${m_file} "${metrics_dir}/run_import_metrics.prom"
exit 0
