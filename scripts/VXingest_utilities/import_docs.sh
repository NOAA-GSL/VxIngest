#!/bin/sh

function usage () {
  echo "Usage $0 -c credentials-file -p full_path_to_json_files_directory, -l log_dir [-n number_of_processes (default 1)]"
  echo "(The number_of_processes must be less than or equal to nproc)."
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "This script assumes that you have cloned VXingest into ${HOME}/VXingest"
  echo "If you cloned it elsewhere, make a link."
  echo "This script uses cbimport with 'number_of_processes' cbimport processes running simultaneously."
  echo "The jason files in 'full_path_to_json_files_directory' will be seperated into (number_of_files / num_processes)"
  echo "groups and imported simultaneously. Output is written to 'logdir/cbimport_n.log' where n is the instance number."
  
  exit 1
}
number_of_processes=1
number_of_cpus=$(nproc)
while getopts 'c:p:n:l:' param
do
    case "${param}" in
        c)
            export credentials_file=${OPTARG}
            if [ ! -f "${credentials_file}" ]; then
              echo "${credentials_file} does not exist" 
              usage
            fi
            ;;
        p)
            export input_file_path=${OPTARG}
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
            export log_dir=${OPTARG}
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
  echo "no log_dir specified"
  usage
fi

if [ "${number_of_processes}" -gt "$number_of_cpus" ]; then
  echo "${number_of_processes} exceeds ${number_of_cpus}"
  usage
fi

export host=$(grep cb_host ${credentials_file} | awk '{print $2}')
export user=$(grep cb_user ${credentials_file} | awk '{print $2}')
export pwd=$(grep cb_password ${credentials_file} | awk '{print $2}')
if [ -z "${host}" ]; then
  echo "credentials do not specify cb_host"
  usage
fi
if [ -z "${user}" ];then
  echo "credentials do not specify cb_user"
  usage
fi
if [ -z "${pwd}" ];then 
  echo "credentials do not specify cb_password"
  usage
fi

function do_import() {
  file_list=$1
  cat ${file_list} | while read f
  do
    cbimport json --cluster couchbase://${host} --bucket mdata --username ${user} --password ${pwd} --format list --generate-key %id% --dataset file:///${f} >> ${log_dir}/${file_list} 2>&1
  done
}

curdir=$(pwd)
tmp_dir=$(mktemp -d -t cbimport_files-XXXXXXXXXX)
cd ${tmp_dir}
find ${input_file_path} -name "*.json" | split -d -l $(( $(find ${input_file_path} -name "*.json" | wc -l) / ${number_of_processes} + 1 ))
# each file is a list of files
ls -1 | while read f
do 
  do_import ${f}
done
cd ${curdir}
rm -rf ${tmp_dir}
