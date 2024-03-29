#!/usr/bin/env bash
#NOTE: this script assumes that if you are running a mac you have installed coreutils
# i.e. brew install coreutils
# so that gdate can be used instead of date
#
#Example....
#/home/amb-verif/VXingest/scripts/VXingest_utilities/run-bdp-model-retro.sh -y2021 -m07 -d01 -h"$(seq -s, -w 00 11)" -f"$(seq -s, -w 00 18)" -t /data -c /home/amb-verif/VxIngest
#
usage() {
     echo "Usage: $0 -y year(2 digit) -m month (2 digit) -d day (2 digit) -h hours (comma seperated list) -f fcst_hours (comma seperated list) -t target directory -c (clone directory)"1>&2; exit 1; 
     echo example '/home/amb-verif/VXingest/scripts/VXingest_utilities/run-bdp-model-retro.sh -y2021 -m07 -d01 -h"$(seq -s, -w 00 11)" -f"$(seq -s, -w 00 18)" -t /data -c /home/amb-verif/VxIngest'
}
uname -a | grep -i Darwin > /dev/null
if [ $? -eq 0 ]
then
   mydate=gdate
else
  mydate=date
fi
$mydate > /dev/null
if [[ $? -ne 0 ]]; then
    echo 'your date command is broken (if you are on a mac have you "brew install coreutils" ?)'
    exit 1
fi

check_param() {
    x=$1
    l=$2
    re='^[0-9]+$'
    if ! [[ $x =~ $re ]]; then
        echo "error: ${x} is not a number" >&2
        usage
    fi
    if [ ${#x} -ne $l ]; then
        echo "error: ${x} is not 2 digits" >&2
        usage
    fi
}

while getopts "y:m:d:h:f:t:c:" o; do
    case "${o}" in
        y)
            year=${OPTARG}
            check_param $year 4
            ;;
        m)
            month=${OPTARG}
            check_param $month 2
            ;;
        d)
            day=${OPTARG}
            check_param $day 2
            ;;
        h)
            hours_str=${OPTARG}
            IFS=',' read -ra hours <<< "$hours_str"
            for hr in "${hours[@]}"
                do
                  check_param $hr 2
                done
            ;;
        f)
            fcst_hours_str=${OPTARG}
            IFS=',' read -ra fcst_hours <<< "$fcst_hours_str"
            for hr in "${fcst_hours[@]}"
                do
                  check_param $hr 2
                done
            ;;
        t)
            target_dir=${OPTARG}
            if [[ ! -d "${target_dir}" ]]; then
                echo "error: ${target_dir} is not a directory" >&2
                usage
            fi
            ;;
        c)
            clonedir=${OPTARG}
            if [[ ! -d "${clonedir}" ]]; then
                echo "error: ${clonedir} is not a directory" >&2
                usage
            fi
            echo clonedir is ${clonedir}
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

if [ -z "${year}" ] || [ -z "${month}" ] || [ -z "${day}" ] || [ -z "${hours}" ] || [ -z "${fcst_hours}" ] || [ -z "${target_dir}" ] || [ -z "${clonedir}" ] ; then
    usage
fi
for hr in "${hours[@]}"; do
    for fc in ${fcst_hours[@]}; do
        aws s3 cp s3://noaa-hrrr-bdp-pds/hrrr.${year}${month}${day}/conus/hrrr.t${hr}z.wrfprsf${fc}.grib2 ${target_dir}/retro-hrrr-${year}${month}${day}/$(${mydate} --date=${year}${month}${day} +%y%j)${hr}0000${fc} --no-sign-request --no-progress --only-show-errors
    done
done
${clonedir}/scripts/VXingest_utilities/run-cron-models.sh -c ${HOME}/VxIngest -t ${target_dir}/retro-hrrr-${year}${month}${day}/ -o ${target_dir}/retro-hrrr-${year}${month}${day}-output
rm -rf ${target_dir}/retro-hrrr-${year}${month}${day}
