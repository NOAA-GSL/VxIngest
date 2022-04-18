#!/usr/bin/env sh

echo STARTING
date
if [ $# -ne 1 ]; then
        echo "Usage $0 VxIngest_clonedir";
        exit 1
fi
clonedir=$1
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

if [ ! -d "${HOME}/logs" ]; then
        mkdir ${HOME}/logs
fi

echo "--------"
echo "*************************************"
echo "netcdf obs and stations"
outdir="/data/netcdf_to_cb/output/${pid}"
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
python ${clonedir}/grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/hrrr/conus/wrfprs/grib2 -m %y%j%H%f -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

echo "*************************************"
echo "rap_ops_130"
outdir="/data/grib2_to_cb/rap_ops_130/output/${pid}"
python ${clonedir}/grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_rap_ops_130_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/rap/iso_130/grib2 -m %y%j%H%f -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

echo "--------"
echo "ctc's"
echo "*************************************"
outdir="/data/ctc_to_cb/output/${pid}"
python ${clonedir}/ctc_to_cb/run_ingest_threads.py -s /data/ctc_to_cb/load_specs/load_spec_metar_ctc_V01.yaml  -c ~/adb-cb1-credentials -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

#echo "*************************************"
#echo "legacy ctc's"
#outdir="/data/ctc_to_cb-legacy/output/${pid}"
#python ${clonedir}/ctc_to_cb/run_ingest_threads.py -s /data/ctc_to_cb/load_specs/load_spec_metar_ctc_legacy_V01.yaml  -c ~/adb-cb1-credentials -o $outdir -t8
#${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

echo "--------"
date
echo "*************************************"
echo "update metadata"
${clonedir}/mats_metadata_and_indexes/metadata_files/update_ceiling_mats_metadata.sh ~/adb-cb1-credentials
echo "FINISHED"
date
