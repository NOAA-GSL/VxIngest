#!/usr/bin/env sh

echo STARTING 
date
if [ $# -ne 1 ]; then
        echo "Usage $0 VxIngest_clonedir";
        exit 1
fi
clonedir=$1

cd ${clonedir} && export PYTHONPATH=`pwd`
gitroot=$(git rev-parse --show-toplevel)
if [ "$gitroot" != "$(pwd)" ];then
        echo "$(pwd) is not a git root directory: Usage $0 VxIngest_clonedir"
        exit
fi

if [ ! -d "$clonedir/logs" ]; then
        mkdir ${clonedir}/logs
fi

echo "*************************************"
echo "netcdf obs and stations"
rm -rf /data/netcdf_to_cb/output/*
python netcdf_to_cb/run_ingest_threads.py -s /data/netcdf_to_cb/load_specs/load_spec_netcdf_metar_obs_V01.yaml -c ~/adb-cb1-credentials -p /public/data/madis/point/metar/netcdf -m %Y%m%d_%H%M -o /data/netcdf_to_cb/output -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p /data/netcdf_to_cb/output -n 8 -l ${clonedir}/logs

echo "*************************************"
echo "models"
echo "hrrr_ops"
rm -rf /data/grib2_to_cb/hrrr_ops/output/*
python grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/hrrr/conus/wrfprs/grib2 -m %y%j%H%f -o /data/grib2_to_cb/hrrr_ops/output -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p /data/grib2_to_cb/hrrr_ops/output -n 8 -l ${clonedir}/logs

echo "*************************************"
echo "rap_ops_130"
rm -rf /data/grib2_to_cb/rap_ops_130/output/*
python grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_rap_ops_130_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/rap/iso_130/grib2 -m %y%j%H%f -o /data/grib2_to_cb/rap_ops_130/output -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p /data/grib2_to_cb/rap_ops_130/output -n 8 -l ${clonedir}/logs

echo "*************************************"
echo "ctc's"
rm -rf /data/ctc_to_cb/output/*
python ctc_to_cb/run_ingest_threads.py -s /data/ctc_to_cb/load_specs/load_spec_metar_ctc_V01.yaml  -c ~/adb-cb1-credentials -o /data/ctc_to_cb/output -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p /data/ctc_to_cb/output -n 8 -l ${clonedir}/logs

echo "FINISHED" 
date
