#!/usr/bin/env bash
# use the following for debugging (to stop with read)
#TTY=`tty`

# Check the import directory to see if any new tarballs are there.
# If there are any, one by one untar them into their own temp directory
# and import the data documents, there should be data files and one log file in each,
# Creat an import log after the name of the associated log file (from the tarball).
# Scrape the logfiles and update the metrics.
# On success destroy each temporary directory and archive the associated tarball.
# On failure archive the tarball, and destroy the temp directory.
# This script expects to execute inside the clone directory of the VxIngest repo.
# This script expects to be run as user amb-verif very frequently (like two minutes).
# This script expects to have a python virtual environment in the amb-verif home directory in the subdirectory vxingest-env.
# This script will generate metrics to track its success or failure, and to track how frequently it runs.

function usage {
  echo "Usage $0 -c credentials-file -d VxIngest directory -l load directory -t temp_dir -m metrics_directory"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "The VxIngest directory specifies the directory where The VxIngest repo has been cloned."
  echo "The load directory is where the program will look for the tar files"
  echo "The tar_dir directory is where the program will unbundle the tar files (in uniq temporary subdirs)"
  echo "The metrics directory is where the scraper will place the metrics"
  echo "This script expects to execute inside the clone directory of the VxIngest repo"
  echo "This script expects to be run as user amb-verif"
  echo "This script expects to have a python virtual environment in the amb-verif home directory in the subdirectory vxingest-env"
  failed_import_count=$((failed_import_count+1))
  exit 1
}

success_import_count=0
failed_import_count=0
success_scrape_count=0
failed_scrape_count=0
start=$(date +%s)

while getopts 'c:d:l:t:m:' param; do
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
  d)
    # remove the last '/' if it is there
    export clonedir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${clonedir}" ]]; then
      echo "$0 ERROR: VxIngest clone directory ${clonedir} does not exist"
      usage
    fi
    ;;
  l)
    # remove the last '/' if it is there
    export load_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${load_dir}" ]]; then
      echo "$0 ERROR: Work load directory ${load_dir} does not exist"
      usage
    fi
    ;;
  t)
    # remove the last '/' if it is there
    export tar_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${tar_dir}" ]]; then
      echo "$0 ERROR: tar file directory ${tar_dir} does not exist"
      usage
    fi
    ;;
  m)
    # remove the last '/' if it is there
    export metrics_dir=$(echo "${OPTARG}" | sed 's|/$||')
    if [[ ! -d "${metrics_dir}" ]]; then
      echo "$0 ERROR: VxIngest metrics directory ${metrics_dir} does not exist"
      usage
    fi
    ;;
  *)
    echo "$0 ERROR: wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done
if [[ -z ${credentials_file} ]] || [[ -z ${clonedir} ]] || [[ -z ${load_dir} ]] || [[ -z ${metrics_dir} ]] || [[ -z ${tar_dir} ]]; then
  echo "$0 *missing parameter*"
  echo "$0 provided credentials_file is ${credentials_file}"
  echo "$0 provided clonedir is ${clonedir}"
  echo "$0 provided load_dir is ${load_dir}"
  echo "$0 provided metrics_dir is ${metrics_dir}"
  echo "$0 provided tar_dir is ${tar_dir}"
  usage
fi

pid=$$
if [ "$(whoami)" != "amb-verif" ]; then
        echo "$0 Script must be run as user: amb-verif"
        usage
fi

source ${HOME}/vxingest-env/bin/activate

cd ${clonedir} && export PYTHONPATH=`pwd`
gitroot=$(git rev-parse --show-toplevel)
if [ "$gitroot" != "$(pwd)" ];then
        echo "$0 $(pwd) is not a git root directory: Usage $0 VxIngest_clonedir"
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
  echo "$0 processing the tar file ${f}"
  t_dir=$(mktemp -d -p ${tar_dir})
  echo "$0 extracting tarball ${f} to temp_dir ${t_dir}"
  echo "$0 tar -xzf ${f} -C ${t_dir}"
  tar -xzf "${load_dir}/${f}" -C ${t_dir}
  echo "$0 finished extracting tarball ${f}"
  log_file_count=`ls -1 ${t_dir}/*.log | wc -l`
  if [[ ${log_file_count} -ne 1 ]]; then
    echo "$0 There is not just one log_file in ${t_dir} - extracted from ${f} - there are ${log_file_count}"
    echo "$0 moving tar file ${f} to ${archive_dir}"
    echo "$0  - exiting"
    failed_import_count=$((failed_import_count+1))
    mv $f $archive_dir
    echo "$0 removing temp_dir ${t_dir}"
    rm -rf ${t_dir}
    usage
  fi
  # ok - have one log file
  log_file=`ls -1 ${t_dir}/*.log`
  echo "$0 processing log_file ${log_file}"
  log_dir=$(dirname ${log_file})
  log_file_name=$(basename $log_file)
  # create a temporary import log file so that there isn't any chance of two imports using the same file somehow.
  # This import log file will get copied into the main log directory.
  import_log_file="${log_dir}/import-${log_file_name}"
  echo "$0 import log file will be: ${import_log_file}"
  # create a temporary update log file so that there isn't any chance of two imports using the same file somehow.
  # This update log file will get copied into the main log directory.
  update_metadata_log_file="${log_dir}/update_metadata-${log_file_name}"
  echo "$0 update metadata  log file will be: ${update_metadata_log_file}"
  # run the import job
  metric_name=$(grep metric_name ${log_file} | awk '{print $2}')
  echo "$0 metric name will be ${metric_name}"
  import_metric_name="import_${metric_name}"
  echo "$0 import metric name will be ${import_metric_name}"
  echo "$0 metric_name ${import_metric_name}" > ${import_log_file}
  echo "$0 RUNNING - ${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ${credentials_file} -p ${t_dir} -n 8 -l ${clonedir}/logs >> ${import_log_file}"
#read -p "enter to continue import_docs" <$TTY
  ${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ${credentials_file} -p ${t_dir} -n 8 -l ${clonedir}/logs >> ${import_log_file} 2>&1
  exit_code=$?
  wait
  echo "$0 exit_code:${exit_code}" >> ${import_log_file}
  if [[ "${exit_code}" -ne "0" ]]; then
    failed_import_count=$((failed_import_count+1))
    echo "$0 import failed for $f"
    echo "$0 moving tar file ${f} to ${archive_dir}"
    mv $f $archive_dir
    # don't exit - let the scraper record the error
  else
    success_import_count=$((success_import_count+1))
  fi
  echo "$0 run-import.sh success_import_count from import_docs.sh is ${success_import_count}"

  # run the scraper
  sleep 2  # eventually consistent data - give it a little time
#read -p "enter to continue scrapper" <$TTY
  ${clonedir}/scripts/VXingest_utilities/scrape_metrics.sh -c ${credentials_file} -l ${log_file} -d ${metrics_dir} >> ${import_log_file} 2>&1
  exit_code=$?
  echo "$0 finished running scraper"
#read -p "enter to continue to end" <$TTY
  if [[ "${exit_code}" -ne "0" ]]; then
    failed_scrape_count=$((failed_scrape_count+1))
  else
    success_scrape_count=$((success_scrape_count+1))
  fi
  #save the temporary log files
  cp ${import_log_file} ${clonedir}/logs
  echo "$0 --------"
  # now clean up the files
  # remove the tar file (if it failed it should have been archived)
  echo "$0 removing tar file - $f"
  #rm ${load_dir}/${f}
  mv ${load_dir}/${f} $archive_dir
  # remove the data files ($t_dir)
  echo "$0 removing data directory - ${t_dir}"
  rm -rf ${t_dir}
done

echo "$0 *************************************"

echo "$0 success import count is ${success_import_count}"
if [[ "${success_import_count}" -ne "0" ]]; then
	echo "$0 attempting to update metadata"
	LOCKDIR="/data/import_lock"
	if mkdir -- "$LOCKDIR"; then
	    echo "$0 update ceiling metadata log file is ${update_metadata_log_file}"
	    ${clonedir}/mats_metadata_and_indexes/metadata_files/update_ceiling_mats_metadata.sh ${credentials_file} >> ${update_metadata_log_file}
            ret=$?
            if [[ "${ret}" -ne "0" ]]; then
               echo "$0 ceiling metadata update failed with exit code ${ret}"
            fi
            echo "$0 update visibility metadata log file is ${update_metadata_log_file}"
	    ${clonedir}/mats_metadata_and_indexes/metadata_files/update_visibility_mats_metadata.sh ${credentials_file} >> ${update_metadata_log_file}
            ret=$?
            if [[ "${ret}" -ne "0" ]]; then
               echo "$0 visibility import failed with exit code ${ret}"
            fi
	    if rmdir -- "$LOCKDIR"
	    then
		echo "$0 import finished"
	    else
		echo "$0 IMPORT ERROR: Could not remove import lock dir" >&2
	    fi
            cp ${update_metadata_log_file} ${clonedir}/logs
	else
            echo "$0 import_lock exists - cannot update metadata"
	fi
fi
echo "$0 FINISHED"
end="$(date +%s)"
m_file=$(mktemp)
echo "run_import_duration $((end-start))" > ${m_file}
echo "run_import_success_count ${success_import_count}" >> ${m_file}
echo "run_import_failure_count ${failed_import_count}" >> ${m_file}
echo "run_scrape_success_count ${success_scrape_count}" >> ${m_file}
echo "run_scrape_failure_count ${failed_scrape_count}" >> ${m_file}
cp ${m_file} "${metrics_dir}/run_import_metrics.prom"
rm ${m_file}
exit 0
