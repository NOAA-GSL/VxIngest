#!/usr/bin/env sh

echo STARTING 
date
if [ $# -ne 1 ]; then
        echo "Usage $0 VxIngest_clonedir";
        exit 1
fi
clonedir=$1

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

if [ ! -d "${HOME}/logs" ]; then
        mkdir ${HOME}/logs-retro
fi

if [ ! -d "${clonedir}/logs-retro" ]; then
        mkdir ${clonedir}/logs-retro
fi

echo "*************************************"
echo "netcdf obs and stations"
#rm -rf /data/netcdf_to_cb-retro/output/*
python netcdf_to_cb/run_ingest_threads.py -s /data/netcdf_to_cb-retro/load_specs/load_spec_netcdf_metar_retro_obs_V01.yaml -c ~/adb-cb1-credentials -p /couchbase/xfer/ -m %Y%m%d_%H%M -o /data/netcdf_to_cb-retro/output -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p /data/netcdf_to_cb-retro/output -n 8 -l ${clonedir}/logs-retro

echo "*************************************"
echo "update metadata"
${clonedir}/mats_metadata_and_indexes/metadata_files/update_ceiling_mats_metadata.sh ~/adb-cb1-credentials
echo "FINISHED"
date
