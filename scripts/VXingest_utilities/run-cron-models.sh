#!/usr/bin/env sh

echo STARTING
date
if [ $# -ne 1 ]; then
        echo "Usage $0 -t target_dir -o output_dir -c VxIngest_clonedir";
        exit 1
fi

while getopts "t:c:o:" opt; do
    case "${opt}" in
        t)
            target_dir=${OPTARG}
            if [[ ! -d "${target_dir}" ]]; then
                echo "error: ${target_dir} is not a directory" >&2
                usage
            fi
            ;;
        o)
            output_dir=${OPTARG}
            if [[ ! -d "${output_dir}" ]]; then
                echo "error: ${output_dir} is not a directory" >&2
                usage
            fi
            ;;
        c)
            clonedir=${OPTARG}
            if [[ ! -d "${clonedir}" ]]; then
                echo "error: ${clonedir} is not a directory" >&2
                usage
            fi
            cd ${clonedir}
            gitroot=$(git rev-parse --show-toplevel)
            if [ "$gitroot" != "$(pwd)" ];then
                    echo "$(pwd) is not a git root directory:"
                    usage
            fi
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${target_dir}" ] || [ -z "${output_dir}" ] ; then
    usage
fi


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
echo "models"
echo "hrrr_ops"
rm -rf /data/grib2_to_cb/hrrr_ops/output/*
python grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p ${target_dir} -m %y%j%H%f -o ${output_dir} -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p ${output_dir} -n 8 -l ${clonedir}/logs

echo "FINISHED"
date
