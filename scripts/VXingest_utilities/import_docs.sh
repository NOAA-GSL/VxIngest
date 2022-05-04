#!/usr/bin/env bash

function usage() {
  echo "Usage $0 -c credentials-file -p full_path_to_json_files_directory, -l log_dir [-n number_of_processes (default 1)]"
  echo "(The number_of_processes must be less than or equal to nproc)."
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "This script assumes that you have cloned VXingest into ${HOME}/VXingest"
  echo "If you cloned it elsewhere, make a link."
  echo "This script uses cbimport with 'number_of_processes' cbimport processes running simultaneously."
  echo "The jason files in 'full_path_to_json_files_directory' will be seperated into (number_of_files / num_processes)"
  echo "groups and imported simultaneously. Output is written to 'logdir/cbimport_n.log' where n is the instance number."
  echo "sample invocation...."
  echo "${HOME}VXingest/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p /data/grib2_to_cb/output -n 8 -l ${HOME}/VXingest/logs"
  exit 1
}
number_of_processes=1
number_of_cpus=$(nproc)
while getopts 'c:p:n:l:' param; do
  case "${param}" in
  c)
    credentials_file=${OPTARG}
    if [ ! -f "${credentials_file}" ]; then
      echo "${credentials_file} does not exist"
      usage
    fi
    ;;
  p)
    input_file_path=${OPTARG}
    if [ ! -d "${input_file_path}" ]; then
      echo "${input_file_path} does not exist"
      usage
    fi
    ;;
  n)
    number_of_processes=${OPTARG}
    if [ ! "${number_of_processes}" -le "${number_of_cpus}" ]; then
      echo "${number_of_processes} exceeds ${number_of_cpus}"
      usage
    fi
    ;;
  l)
    log_dir=${OPTARG}
    if [ ! -d "${log_dir}" ]; then
      echo "${log_dir} does not exist"
      usage
    fi
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
if [ ! -d "${input_file_path}" ]; then
  echo "no input_file_path specified"
  usage
fi
if [ ! -d "${log_dir}" ]; then
  echo "no log_dir specified - using stdout"
fi

if [ "${number_of_processes}" -gt "$number_of_cpus" ]; then
  echo "${number_of_processes} exceeds ${number_of_cpus}"
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

do_import() {
  file_list=$1
  sleep 10
  cat ${file_list} | while read f; do
    #echo 'cbimport json --cluster couchbase://${host} --bucket mdata --username ${user} --password ${pwd} --format list --generate-key %id% --dataset file:///${f}'
    /opt/couchbase/bin/cbimport json --cluster couchbase://${host} --bucket mdata --username ${user} --password ${pwd} --format list --generate-key %id% --dataset file:///${f}
  done
}

curdir=$(pwd)
tmp_dir=$(mktemp -d -t cbimport_files-XXXXXXXXXX)
cd ${tmp_dir}
# create a tmp log dir so that multiple instances will not step on each other
tmp_log_dir="${tmp_dir}/logs"
mkdir ${tmp_log_dir}
find ${input_file_path} -name "*.json" | split -d -l $(($(find ${input_file_path} -name "*.json" | wc -l) / ${number_of_processes} + 1))
# each file is a list of files
for f in ${tmp_dir}/*; do
    fname=$(basename ${f})
    do_import ${f} > ${tmp_log_dir}/${fname} 2>&1 &
done
echo "cbimport commands submitted, now waiting"
wait
echo "cbimport commands submitted, done waiting"
cd ${curdir}
grep -i successfully ${tmp_log_dir}/x* | awk '{print $2}' | awk 'BEGIN { FS="file:///" }; {print $2}' | tr -d "\`" | while read f_input;do
rm -rf $f_input
done
#remove empty input file_paths
find ${input_file_path} -maxdepth 0 -empty -exec rm -rf ${input_file_path} \;
# copy logs and remove tmp_dir just to be sure
cp -a ${tmp_log_dir} ${log_dir}
rm -rf ${tmp_dir}
