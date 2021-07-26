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
            if [ ! -f "$credentials_file" ]; then
              echo "$credentials_file does not exist"; 
              usage
            fi
            ;;
        p)
            export input_file=${OPTARG}
            if [ ! -d "$input_file_path" ]; then 
              echo "$input_file_path does not exist"; 
              usage
            fi
            ;;
        n)
            number_of_processes=${OPTARG}
            if [ ! "$number_of_processes" -le "$number_of_cpus" ]; then
              echo "$number_of_processes exceeds $number_of_cpus";i
              usage
            fi
            ;;
        l)
            export log_dir=${OPTARG}
            if [ ! -d "$log_dir" ]; then
              echo "$log_dir does not exist"; 
              usage
            fi
            ;;
        *)
            echo "wrong parameter, I don't do $param"; 
            usage 
            ;;
    esac
done
shift $((OPTIND-1))

[ -f "$credentials_file" ] || echo "no credentials_file specified"; usage
[ -d "$input_file_path" ] || echo "no input_file_path specified"; usage
[ -d "$log_dir" ] || echo "no log_dir specified"
[ "$number_of_processes" -le "$number_of_cpus" ] || echo "$number_of_processes exceeds $number_of_cpus"; usage
export host=$(grep cb_host ${credentials} | awk '{print $2}')
export user=$(grep cb_user ${credentials} | awk '{print $2}')
export pwd=$(grep cb_password ${credentials} | awk '{print $2}')
[ -z "$host" ] &&  echo "credentials do not specify cb_host"; usage
[ -z "$user" ] &&  echo "credentials do not specify cb_user"; usage
[ -z "$pwd" ] &&  echo "credentials do not specify cb_password"; usage

function do_import () {
  file_list=$1
  echo $file_list | while read f
  do
    [ -f "$credentials_file" ] || echo "$f does not exist"; return
    echo cbimport json --cluster couchbase://${host} --bucket mdata --username ${user} --password ${pwd} --format list --generate-key %id% --dataset file:///${f} > $log_dir/
  done
}

curdir=$(pwd)
tmp_dir=$(mktemp -d -t cbimport_files-XXXXXXXXXX)
cd $tmp_dir
groups=$(find ${input_file_path}/*.json | split -l $(( $(ls -1 ../*.json | wc -l) / $number_of_processes + 1 )))
ls -1 | while read f do 
  do_import $f
done
cd $curdir
