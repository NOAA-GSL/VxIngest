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
  echo "Usage $0 -c credentials-file -d VxIngest directory"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "The VxIngest directory specifies " directory where The VxIngest repo has been cloned into.
  echo "This script expects to execute inside the clone directory of the VxIngest repo"
  echo "This script expects to be run as user amb-verif"
  echo "This script expects to have a python virtual environment in the amb-verif home directory in the subdirectory vxingest-env"
  exit 1
}

while getopts 'c:d:' param; do
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
    clonedir=${OPTARG}
    if [[ ! -d "${clonedir}" ]]; then
      echo "ERROR: VxIngest clone directory ${clonedir} does not exist"
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
        exit
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

#SELECT META().sd AS id
#       run_priority,
#       interval_minutes
#       offset_minutes
#FROM mdata
#WHERE type='JOB'
#    AND version='V01'
#    AND status='active'
#    AND ((<this_run_hour = 0) OR ((interval_minutes / 60) % <this_run_hour>) = 0)
#    AND ((interval minutes % 60) / 15) = <this_run_quarter>
#ORDER BY run_minute, offset_minutes, run_priority

job_docs=$(curl -s http://adb-cb1.gsd.esrl.noaa.gov:8093/query/service -u"${cred}" -d "statement=select meta().id as id, run_priority, interval_minutes, offset_minutes from mdata where type='JOB' AND version='V01' AND status='active'" | jq -r '.results | .[]')








if [ ! -d "${HOME}/logs" ]; then
        mkdir ${HOME}/logs
fi

echo "--------"
echo "*************************************"
echo "netcdf obs and stations"
outdir="/data/netcdf_to_cb/output/${pid}"
mkdir $outdir
python ${clonedir}/netcdf_to_cb/run_ingest_threads.py -s /data/netcdf_to_cb/load_specs/load_spec_netcdf_metar_obs_V01.yaml -c ~/adb-cb1-credentials -p /public/data/madis/point/metar/netcdf -m %Y%m%d_%H%M -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

#echo "*************************************"
#echo "netcdf legacy obs and stations"
#outdir="/data/netcdf_to_cb-legacy/output/${pid}"
#python ${clonedir}/netcdf_to_cb/run_ingest_threads.py -s /data/netcdf_to_cb/load_specs/load_spec_netcdf_metar_legacy_obs_V01.yaml -c ~/adb-cb1-credentials -p /public/data/madis/point/metar/netcdf -m %Y%m%d_%H%M -o $outdir -t8
#${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

echo "--------"
echo "models"
echo "*************************************"
echo "hrrr_ops"
outdir="/data/grib2_to_cb/hrrr_ops/output/${pid}"
mkdir $outdir
python ${clonedir}/grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/hrrr/conus/wrfprs/grib2 -m %y%j%H%f -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

echo "*************************************"
echo "rap_ops_130"
outdir="/data/grib2_to_cb/rap_ops_130/output/${pid}"
mkdir $outdir
python ${clonedir}/grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_rap_ops_130_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/rap/iso_130/grib2 -m %y%j%H%f -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

echo "--------"
echo "ctc's"
echo "*************************************"
echo "hrrr_ops rap_ops_130"
outdir="/data/ctc_to_cb/output/${pid}"
mkdir $outdir
python ${clonedir}/ctc_to_cb/run_ingest_threads.py -s /data/ctc_to_cb/load_specs/load_spec_metar_ctc_V01.yaml  -c ~/adb-cb1-credentials -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

echo "--------"
date
echo "*************************************"
echo "update metadata"
${clonedir}/mats_metadata_and_indexes/metadata_files/update_ceiling_mats_metadata.sh ~/adb-cb1-credentials
echo "FINISHED"
date
