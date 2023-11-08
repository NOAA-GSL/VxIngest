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
  echo "The tar_dir directory is where the program will unbundle the tar files (in uniq temporary subdirs)"
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
    export tar_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${tar_dir}" ]]; then
      echo "ERROR: tar file directory ${tar_dir} does not exist"
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
if [[ -z ${credentials_file} ]] || [[ -z ${load_dir} ]] || [[ -z ${metrics_dir} ]] || [[ -z ${tar_dir} ]]; then
  echo "*missing parameter*"
  echo "provided credentials_file is ${credentials_file}"
  echo "provided load_dir is ${load_dir}"
  echo "provided metrics_dir is ${metrics_dir}"
  echo "provided tar_dir is ${tar_dir}"
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
archive_dir="${tar_dir}/archive"
mkdir -p "${archive_dir}"
runtime=`date +\%Y-\%m-\%d:\%H:\%M:\%S`
ls -1 ${load_dir} | while read f; do
  echo "processing the tar file ${f}"
  t_dir=$(mktemp -d -p ${tar_dir})
  echo "extracting tarball ${f} to temp_dir ${t_dir}"
  echo "tar -xzf ${f} -C ${t_dir}"
  tar -xzf "${load_dir}/${f}" -C ${t_dir}
  echo "finished extracting tarball ${f}"
  log_file_count=`ls -1 ${t_dir}/*.log | wc -l`
  if [[ ${log_file_count} -ne 1 ]]; then
    echo "There is not just one log_file in ${t_dir} - extracted from ${f} - there are ${log_file_count}"
    echo "moving tar file ${f} to ${archive_dir}"
    echo " - exiting"
    failed_import_count=$((failed_import_count+1))
    mv $f $archive_dir
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
  metric_name=$(grep metric_name ${log_file} | awk '{print $2}')
  echo "metric name will be ${metric_name}"
  import_metric_name="import_${metric_name}"
  echo "import metric name will be ${import_metric_name}"
  echo "metric_name ${import_metric_name}" > ${import_log_file}
  echo "RUNNING - ${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ${credentials_file} -p ${t_dir} -n 8 -l ${clonedir}/logs >> ${import_log_file}"
  ${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ${credentials_file} -p ${t_dir} -n 8 -l ${clonedir}/logs >> ${import_log_file} 2>&1
  exit_code=$?
  wait
  echo "exit_code:${exit_code}" >> ${import_log_file}
  if [[ "${exit_code}" -ne "0" ]]; then
    failed_import_count=$((failed_import_count+1))
    echo "import failed for $f"
    echo "moving tar file ${f} to ${archive_dir}"
    mv $f $archive_dir
    # don't exit - let the scraper record the error
  else
    success_import_count=$((success_import_count+1))
  fi

  # run the scraper
  sleep 2  # eventually consistent data - give it a little time
  echo "RUNNING - ${clonedir}/scripts/VXingest_utilities/scrape_metrics.sh -c ${credentials_file} -l ${log_file} -d ${metrics_dir}"
  ${clonedir}/scripts/VXingest_utilities/scrape_metrics.sh -c ${credentials_file} -l ${log_file} -d ${metrics_dir}
  exit_code=$?
  if [[ "${exit_code}" -ne "0" ]]; then
    failed_scrape_count=$((failed_scrape_count+1))
  else
    success_scrape_count=$((success_scrape_count+1))
  fi
  # save the import log file
  cp ${import_log_file} ${clonedir}/logs
  echo "--------"
  # now clean up the files
  # remove the tar file (if it failed it should have been archived)
  echo "removing tar file - $f"
  rm ${load_dir}/${f}
  # remove the data files ($t_dir)
  echo "removing data directory - ${t_dir}"
  rm -rf ${t_dir}
done

echo "*************************************"

if [[ "${success_import_count}" -ne "0" ]]; then
	LOCKDIR="/data/import_lock"
  #if LOCKDIR is > 48 * 3600 seconds old, remove it
  if (( $(date "+%s") - $(date -r ${LOCKDIR} "+%s") > $(( 48 * 3600 )) )); then
    echo "removing old lock file"
    rm -rf ${LOCKDIR}
  fi
	if mkdir -- "$LOCKDIR"; then
	    echo "update ceiling metadata"
	    ${clonedir}/mats_metadata_and_indexes/metadata_files/update_ctc_ceiling_mats_metadata.sh ${credentials_file}
            ret=$?
            if [[ "${ret}" -ne "0" ]]; then
               echo "ceiling metadata update failed with exit code ${ret}"
            fi
            echo "update ceiling metadata"
	    ${clonedir}/mats_metadata_and_indexes/metadata_files/update_ctc_visibility_mats_metadata.sh ${credentials_file}
            ret=$?
            if [[ "${ret}" -ne "0" ]]; then
               echo "visibility import failed with exit code ${ret}"
            fi
            echo "update visibility metadata"
	    ${clonedir}/mats_metadata_and_indexes/metadata_files/update_sums_surface_mats_metadata.sh ${credentials_file}
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
fi
echo "FINISHED"
end=$(date +%s)
m_file=$(mktemp)
echo "run_import_duration $((end-start))" > ${m_file}
echo "run_import_success_count ${success_import_count}" >> ${m_file}
echo "run_import_failure_count ${failed_import_count}" >> ${m_file}
echo "run_scrape_success_count ${success_scrape_count}" >> ${m_file}
echo "run_scrape_failure_count ${failed_scrape_count}" >> ${m_file}
cp ${m_file} "${metrics_dir}/run_import_metrics.prom"
rm ${m_file}
# purge the old files
rm -rf $(for d in $(find /data -type d -name "*purge"); do find $d -type f -mtime +3 -print; done)
exit 0
