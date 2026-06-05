#!/usr/bin/env bash
# Check the import directory to see if any new tarballs are there.
# If there are any, one by one untar them into their own temp directory
# and import the data documents, there should be data files and one log file in each,
# Create an import log after the name of the associated log file (from the tarball).
# On success destroy each temporary directory and the associated tarball.
# On failure archive the tarball, and destroy the temp directory.
# This script expects to execute inside the VxIngest directory.
# This script expects to be run as user amb-verif very frequently (like two minutes).
# This script expects to have a python virtual environment in the amb-verif home directory in the subdirectory vxingest-env.
#
# NOTE If the host in the credentials file contains "cloud.couchbase.com" it will be assumed to be a Capella cluster
# and in that case a cacert_file path MUST be included in the credentials file
# and a certificate must be found in that specified file.
#
# NOTE! nproc is not available on MACOS
# To run this script (on macos) you must make an alias for nproc
# alias nproc="sysctl -n hw.ncpu"
# flock is not available on MACOS
# To run this script on MACOS you must brew install flock

# set the timezone to UTC0 so that the date command will return the correct epoch time
export TZ="UTC0"


function usage {
  echo "Usage $0 -c credentials-file -l load directory -t temp_dir"
  echo "The credentials-file specifies cb_host, cb_user, cb_password and optionally cacert_file (for Capella clusters)."
  echo "The load directory is a directory under ${import_data_dir} where the program will look for the tar files"
  echo "The temp_dir directory a directory under ${import_data_dir} where the program will unbundle the tar files (in uniq temporary subdirs)"
  echo "This script expects to execute inside the VxIngest directory"
  echo "This script expects to be run as user amb-verif"
  exit 1
}
success_import_count=0
failed_import_count=0
# import_data_dir should ne mounted to a host directory. It should have the
# load_dir containing tar files to import, and also the temp_dir where the script will write
# temporary extracted data from the tar balls, and the log_dir where the script will write log files.
import_data_dir="/opt/data_import"
log_dir="${import_data_dir}/logs"
mkdir -p "${log_dir}"
if [[ ! -d "${log_dir}" ]]; then
  echo "ERROR: VxIngest log directory ${log_dir} does not exist"
  usage
fi
if [ ! -w "${log_dir}" ]; then
  echo "log directory ${log_dir} IS NOT WRITABLE"
  usage
fi

function get_credential_value {
  local key=$1
  local file=$2
  awk -v search_key="${key}" '
    /^[[:space:]]*#/ { next }
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (line ~ ("^" search_key "([[:space:]]*:|[[:space:]])")) {
        sub("^" search_key "[[:space:]]*:?", "", line)
        sub(/^[[:space:]]+/, "", line)
        split(line, parts, /[[:space:]]+/)
        print parts[1]
        exit
      }
    }
  ' "${file}"
}

function strip_trailing_slash {
  local path=$1
  if [[ "${path}" != "/" ]]; then
    path=${path%/}
  fi
  printf '%s' "${path}"
}

function import_archive {
  # import the data
  # the data is in the temp directory
  # the log file is in the temp directory
  # the log file is named after the tarball
  f=$1
  archive_dir=$2
  temp_dir=$3
  ca_cert_args=()
  if [[ -n "${4:-}" ]]; then
    ca_cert_args=(--cacert "${4}")
  fi
  t_dir=$(mktemp -d --tmpdir="${temp_dir}")
  if [[ ! -d "${t_dir}" ]]; then
    echo "ERROR: Failed to create VxIngest temp directory ${t_dir}"
    usage
  fi
  # get the subdir from the tarball - all the archives are in a subdir in the tarball
  subdir=$(tar -tzf "${f}" | awk -F'/' 'NR==1 {print $1; exit}')
  echo "processing the tar file ${f}"
  data_dir="${t_dir}/${subdir}"
  echo "extracting tarball ${f} to temp_dir ${t_dir}"
  echo "tar -xzf ${f} -C ${t_dir}"
  # NOTE: the archives are tar'd into a subdirectory so data_dir is t_dir/sub_dir
  if ! tar -xzf "${f}" -C "${t_dir}"; then
    echo "ERROR: tarball ${f} failed to extract"
    base_f=$(basename "${f}")
    echo "moving tar file ${f} to ${archive_dir}/failed-extract-${base_f}"
    failed_import_count=$((failed_import_count + 1))
    # doing cp then rm because of an issue with docker mounts on MAC
    echo cp "${f}" "${archive_dir}/failed-extract-${base_f}"
    mv "${f}" "${archive_dir}/failed-extract-${base_f}"
    rm -rf "${t_dir}"
    return # go to the next tar file
  fi
  echo "finished extracting tarball ${f} to ${t_dir}"
  # run the import job
  # NOTE! nproc is not available on MACOS
  # To run this script you must make an alias for nproc
  # alias nproc="sysctl -n hw.ncpu"
  number_of_cpus=$(nproc)
  json_files=("${data_dir}"/*.json)
  if [[ "${json_files[0]}" == "${data_dir}/*.json" ]]; then
    echo "ERROR: No JSON files found in ${data_dir}"
    failed_import_count=$((failed_import_count + 1))
    base_f=$(basename "${f}")
    mv "${f}" "${archive_dir}/failed-no-json-files-${base_f}"
    rm -rf "${t_dir}"
    return
  fi

  exit_code=0
  for json_f in "${json_files[@]}"; do
    "${HOME}/cbtools/bin/cbimport" json --threads "${number_of_cpus}" --cluster "${cb_host}" --bucket "${bucket}" --scope-collection-exp "${scope}.${collection}" --username "${cb_user}" --password "${cb_pwd}" --format list --generate-key %id% --dataset "file:///${json_f}" "${ca_cert_args[@]}"
    exit_code=$?
    if [[ "${exit_code}" -ne 0 ]]; then
      break
    fi
  done
  if [[ "${exit_code}" -ne "0" ]]; then
    echo "import failed for $f exit_code:${exit_code}"
    failed_import_count=$((failed_import_count + 1))
    base_f=$(basename "${f}")
    echo "moving tar file ${f} to ${archive_dir}/failed-import-${base_f}"
    echo mv "${f}" "${archive_dir}/failed-import-${base_f}"
    mv "${f}" "${archive_dir}/failed-import-${base_f}"
    # don't return or remove the t_dir yet - let the scraper record the error
  else
    echo "import succeeded for $f success_import_count: ${success_import_count}"
    success_import_count=$((success_import_count + 1))
    # save the tar file
    base_f=$(basename "${f}")
    echo "moving tar file ${f} to ${archive_dir}/success-${base_f}"
    echo mv "${f}" "${archive_dir}/success-${base_f}"
    mv "${f}" "${archive_dir}/success-${base_f}"
    # don't return or remove the t_dir yet  - let the scraper record the error
  fi
  echo "removing temp dir ${t_dir}/*"
  rm -rf "${t_dir}"
  echo "--------"
} # end import_archive

# main
# do not allow more than 10 processes to run simultaneously
# If there are already 10 jobs running, don't allow another to start.
if command -v pgrep >/dev/null 2>&1; then
  running_jobs=$(pgrep -fc 'run-import.sh')
else
  running_jobs=$(ps -elf | grep run-import | grep -v grep | wc -l)
fi
if [ "${running_jobs}" -gt 10 ]; then
	echo "too many jobs running - refusing this one"
	exit 1
fi

while getopts 'c:l:t' param; do
  case "${param}" in
  c)
    credentials_file=${OPTARG}
    if [[ ! -f "${credentials_file}" ]]; then
      echo "${credentials_file} does not exist"
      usage
    fi
    cb_host=$(get_credential_value cb_host "${credentials_file}")
    # if it is a multinode host split on ',' and take the first one
    IFS=',' read -ra hostarr <<<"$cb_host"
    cb_host=${hostarr[0]}
    cb_user=$(get_credential_value cb_user "${credentials_file}")
    cb_pwd=$(get_credential_value cb_password "${credentials_file}")
    bucket=$(get_credential_value cb_bucket "${credentials_file}")
    collection=$(get_credential_value cb_collection "${credentials_file}")
    scope=$(get_credential_value cb_scope "${credentials_file}")
    cacert_file=$(get_credential_value cacert_file "${credentials_file}")
    if [[ "${cb_host}" == *"cloud.couchbase.com"* ]]; then
         echo "Host ${cb_host} appears to be a Capella cluster, checking for cacert_file in credentials file"
         if [[ -z "${cacert_file}" ]]; then
             echo "ERROR: No cacert_file specified in credentials file for Capella cluster ${cb_host}, exiting."
             usage
         else
            if ! grep -q "BEGIN CERTIFICATE" "${cacert_file}"; then
                echo "ERROR: cacert_file ${cacert_file} does not appear to contain a valid certificate, exiting."
                usage
            else
                echo "Using cacert_file ${cacert_file} for secure connection to Capella cluster ${cb_host}"
            fi
         fi
    fi
    ;;
  l)
    # remove the last '/' if it is there
    export load_dir=${import_data_dir}/$(strip_trailing_slash "${OPTARG}")
    if [[ ! -d "${load_dir}" ]]; then
      echo "ERROR: Work load directory ${load_dir} does not exist"
      usage
    fi
    ;;
  t)
    # remove the last '/' if it is there
    export temp_dir=${import_data_dir}/$(strip_trailing_slash "${OPTARG}")
    mkdir -p "${temp_dir}"
    if [[ ! -d "${temp_dir}" ]]; then
      echo "ERROR: tar file directory ${temp_dir} does not exist"
      usage
    fi
    ;;
  *)
    echo "ERROR: wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done
if [[ -z "${credentials_file:-}" ]] || [[ -z "${load_dir:-}" ]] || [[ -z "${temp_dir:-}" ]]; then
  echo "*missing parameter*"
  echo "provided credentials_file is ${credentials_file}"
  echo "provided load_dir is ${load_dir}"
  echo "provided temp_dir is ${temp_dir}"
  usage
fi

if [ "$(whoami)" != "amb-verif" ]; then
  echo "Script must be run as user: amb-verif"
  usage
fi

# Check the load directory for new tar balls.
# This script is expected to run in intervals
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

tar_files=("${load_dir}"/*.gz)
if [[ "${tar_files[0]}" == "${load_dir}/*.gz" ]]; then
  tar_files=()
fi

for f in "${tar_files[@]}"; do
  # lock the archive
  if { set -C; 2>/dev/null >"${f}.lock"; }; then
    # set a trap in case of some unexpected exit in import_archive
    trap "rm -f ${f}.lock" EXIT
  else
    echo "skipping ${f} - it is being processed"
    continue
  fi
  import_archive "${f}" "${archive_dir}" "${temp_dir}" "${cacert_file}"
  # unlock the archive (${f} should have already been archived)
  rm "${f}.lock"
done

# update metadata  - currently enabled
update_metadata_enabled="true"
if [ "${update_metadata_enabled}" == "true" ] && [ "${success_import_count}" -ne "0" ]; then
  echo "update metadata import success count: ${success_import_count}"
  METADATAUPDATELOCKDIR="/data/import_lock"
  # If a lock exists but no meta-update is running, treat it as stale and remove it.
  if [[ -d "${METADATAUPDATELOCKDIR}" ]]; then
    if command -v pgrep >/dev/null 2>&1; then
      if pgrep -f 'meta-update' >/dev/null 2>&1; then
        echo "meta-update is already running; skipping metadata update"
        exit 0
      fi
    elif ps -ef | grep -q '[m]eta-update'; then
      echo "meta-update is already running; skipping metadata update"
      exit 0
    fi

    echo "removing stale metadata update lock ${METADATAUPDATELOCKDIR}"
    rm -rf "${METADATAUPDATELOCKDIR}"
  fi
  if mkdir -- "$METADATAUPDATELOCKDIR"; then
    echo "Running meta-update"
    meta_update_exit_code=0
    if cd ./meta_update_middleware; then
      ./meta-update > "${log_dir}/meta-update.log" 2>&1
      meta_update_exit_code=$?
    else
      echo "IMPORT ERROR: Could not change to meta_update_middleware directory" >&2
      meta_update_exit_code=1
    fi

    if ! rmdir -- "$METADATAUPDATELOCKDIR"; then
      echo "IMPORT ERROR: Could not remove import lock dir" >&2
    fi

    if [[ "${meta_update_exit_code}" -eq 0 ]]; then
      echo "import finished"
    else
      echo "meta-update failed with exit code ${meta_update_exit_code}" >&2
    fi
  fi
fi
exit 0
