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
        mkdir ${HOME}/logs
fi

echo "*************************************"
echo "retro ctc's"
rm -rf /data/ctc_to_cb-retro/output/*
python ctc_to_cb/run_ingest_threads.py -s /data/ctc_to_cb/load_specs/load_spec_metar_ctc_retrto_V01.yaml  -c ~/adb-cb1-credentials -o /data/ctc_to_cb-retro/output -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p /data/ctc_to_cb-retro/output -n 8 -l ${clonedir}/logs

echo "FINISHED"
date
