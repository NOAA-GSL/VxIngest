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

function usage {
  echo "Usage $0 -c credentials-file -l load directory -t temp_dir -m metrics_directory"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "The load directory is where the program will look for the tar files"
  echo "The temp_dir directory is where the program will unbundle the tar files (in uniq temporary subdirs)"
  echo "The metrics directory is where the scraper will place the metrics"
  echo "This script expects to execute inside the VxIngest directory"
  echo "This script expects to be run as user amb-verif"
  failed_import_count=$((failed_import_count+1))
  exit 1
}

success_import_count=0
failed_import_count=0
success_scrape_count=0
failed_scrape_count=0
start=$(date +%s)

while getopts 'c:l:t:m:' param; do
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
    read -ra hostarr <<< "$cb_host"
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

pid=$$
if [ "$(whoami)" != "amb-verif" ]; then
    echo "Script must be run as user: amb-verif"
    usage
fi

# Check the load directory for new tar balls.
# This script is expected to run in two minute intervals
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
  echo "archive directory ${archive_dir} IS NOT WRITABLE";
  usage
fi
runtime=`date +\%Y-\%m-\%d:\%H:\%M:\%S`
t_dir="${temp_dir}/${pid}"
mkdir -p "${t_dir}"
if [[ ! -d "${t_dir}" ]]; then
  echo "ERROR: Failed to create VxIngest temp directory ${t_dir}"
  usage
fi
ls -1 ${load_dir}/*.gz | while read f; do
  echo "processing the tar file ${f}"
  echo "extracting tarball ${f} to temp_dir ${t_dir}"
  echo "tar -xzf ${f} -C ${t_dir}"
  # NOTE: the archives are tar'd into a subdirectory so strip-components 1
  tar -xzf "${f}" -C "${t_dir}" --strip-components 1
  if [[ $? != 0 ]]; then
    echo "ERROR: tarball ${f} failed to extract"
    base_f=$(basename $f)
    echo "moving tar file ${f} to ${archive_dir}/failed-extract-${base_f}"
    failed_import_count=$((failed_import_count+1))
    # doing cp then rm because of an issue with docker mounts on MAC
    echo cp $f "${archive_dir}/failed-extract-${base_f}"
    cp $f "${archive_dir}/failed-extract-${base_f}"
    echo rm -rf $f
    rm -rf $f
    echo "removing temp_dir files ${t_dir}/*"
    rm -rf ${t_dir}/*
    continue  # go to the next tar file
  fi
  echo "finished extracting tarball ${f} to ${t_dir}"
  log_file_count=`ls -1 ${t_dir}/*.log | wc -l`
  if [[ ${log_file_count} -ne 1 ]]; then
    echo "There is not just one log_file in ${t_dir} - extracted from ${f} - there are ${log_file_count}"
    base_f=$(basename $f)
    echo "moving tar file ${f} to ${archive_dir}/failred-too-many-log-files-${base_f}"
    echo " - exiting"
    failed_import_count=$((failed_import_count+1))
    # doing cp then rm because of an issue with docker mounts on MAC
    echo cp $f "${archive_dir}/failed-too-many-log-files-${base_f}"
    cp $f "${archive_dir}/failed-too-many-log-files-${base_f}"
    echo rm -rf $f
    rm -rf $f
    echo "removing temp_dir ${t_dir}"
    rm -rf ${t_dir}
    usage
  fi
  # ok - have one log file
  log_file=`ls -1 ${t_dir}/*.log`
  echo "processing log_file ${log_file}"
  log_dir=$(dirname ${log_file})
  log_file_name=$(basename $log_file)
  import_log_file="${log_dir}/import-${log_file_name}"
  echo "import log file will be: ${import_log_file}"
  # run the import job
  metric_name=$(grep metric_name ${log_file} | awk '{print $6}') # Grab the desired column from the python log format
  echo "metric name will be ${metric_name}"
  import_metric_name="import_${metric_name}"
  echo "import metric name will be ${import_metric_name}"
  echo "metric_name ${import_metric_name}" > ${import_log_file}
  echo "RUNNING - scripts/VXingest_utilities/import_docs.sh -c ${credentials_file} -p ${t_dir} -n 6 -l logs >> ${import_log_file}"
  scripts/VXingest_utilities/import_docs.sh -c ${credentials_file} -p ${t_dir} -n $(nproc) -l logs 2>&1 >> ${import_log_file}
  exit_code=$?
  wait
  echo "exit_code:${exit_code}" >> ${import_log_file}
  if [[ "${exit_code}" -ne "0" ]]; then
    echo "import failed for $f exit_code:${exit_code}"
    failed_import_count=$((failed_import_count+1))
    echo "import failed for $f"
    base_f=$(basename $f)
    # doing cp then rm because of an issue with docker mounts on MAC
    echo "moving tar file ${f} to ${archive_dir}/failed-import-${base_f}"
    echo cp $f "${archive_dir}/failed-import-${base_f}"
    cp $f "${archive_dir}/failed-import-${base_f}"
    echo rm -rf $f
    rm -rf $f
    # don't exit - let the scraper record the error
  else
    success_import_count=$((success_import_count+1))
    echo "import succeeded for $f success_import_count: ${success_import_count}"
    # save the tar file
    base_f=$(basename $f)
    echo "moving tar file ${f} to ${archive_dir}/success-${base_f}"
    # doing cp then rm because of an issue with docker mounts on MAC
    echo cp $f "${archive_dir}/success-${base_f}"
    cp $f "${archive_dir}/success-${base_f}"
    echo rm -rf $f
    rm -rf $f
    if [[ $? != 0 ]]; then
      echo "ERROR: failed to move tar file ${f} to ${archive_dir}/success-${base_f}"
      failed_import_count=$((failed_import_count+1))
      # not going to exit - let the scraper record the error
    fi
  fi
  # run the scraper
  sleep 2  # eventually consistent data - give it a little time
  echo "RUNNING - scripts/VXingest_utilities/scrape_metrics.sh -c ${credentials_file} -l ${log_file} -d ${metrics_dir}"
  scripts/VXingest_utilities/scrape_metrics.sh -c ${credentials_file} -l ${log_file} -d ${metrics_dir}
  exit_code=$?
  if [[ "${exit_code}" -ne "0" ]]; then
    failed_scrape_count=$((failed_scrape_count+1))
  else
    success_scrape_count=$((success_scrape_count+1))
  fi
  # archive the log_file
  # note that the log_file is in a subdirectory of the temp_dir
  dirname_log_file=$(dirname $(dirname ${log_file}))
  ls -l ${log_file}
  if [[ ! -d ${dirname_log_file}/archive ]]; then
    mkdir -p ${dirname_log_file}/archive
    chmod 777 ${dirname_log_file}/archive
  fi
  echo mv ${log_file} ${dirname_log_file}/archive
  mv ${log_file} ${dirname_log_file}/archive
  ret=$?
  if [[ ${ret} != 0 ]]; then
    echo "ERROR: failed to move log file ${log_file} to ${dirname_log_file}/archive ret: ${ret}"
  fi
  # archive the import log_file
  ls -l ${import_log_file}
  echo mv ${import_log_file} ${dirname_log_file}/archive
  mv ${import_log_file} ${dirname_log_file}/archive
  ret=$?
  if [[ ${ret} != 0 ]]; then
    echo "ERROR: failed to move import log file ${import_log_file} to ${dirname_log_file}/archive ret: ${ret}"
  fi
  echo "removing temp_dir files ${t_dir}/*"
  rm -rf ${t_dir}/*
  echo "--------"
done
# remove the data dir ($t_dir)
echo "removing data directory - ${t_dir}"
rm -rf ${t_dir}

echo "*************************************"
if [[ "${success_import_count}" -ne "0" ]]; then
  echo "update metadata import success count: ${success_import_count}"
	LOCKDIR="/data/import_lock"
  #if LOCKDIR is > 48 * 3600 seconds old, remove it
  if (( $(date "+%s") - $(date -r ${LOCKDIR} "+%s") > $(( 48 * 3600 )) )); then
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
	    if rmdir -- "$LOCKDIR"
	    then
		echo "import finished"
	    else
		echo "IMPORT ERROR: Could not remove import lock dir" >&2
	    fi
	fi
else
  echo "no new data to import - import success count: ${success_import_count}"
fi
echo "capture metrics"
end=$(date +%s)
m_file=$(mktemp)
echo "run_import_duration $((end-start))" > ${m_file}
echo "run_import_success_count ${success_import_count}" >> ${m_file}
echo "run_import_failure_count ${failed_import_count}" >> ${m_file}
echo "run_scrape_success_count ${success_scrape_count}" >> ${m_file}
echo "run_scrape_failure_count ${failed_scrape_count}" >> ${m_file}
cp ${m_file} "${metrics_dir}/run_import_metrics.prom"
rm ${m_file}
echo "FINISHED"
exit 0
