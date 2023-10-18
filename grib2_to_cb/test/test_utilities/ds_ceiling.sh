#!/bin/bash
if [[ -z ${data} ]]
then
    export data = "/opt/data"
fi
export queue_element="${data}/grib2_to_cb/hrrr_ops/input_files/2128723000002"
echo "Ceiling"
echo
for j in 10 20 30 40 50 150 250 350 450 550 650 750
do
    echo -e -n "$j\t"
    wgrib2 -d 655 -ij 11 $j ${queue_element} | awk -F'=' '{print $2}'
    echo
done

for i in 5 25 45 65 85 105 135 155 175 195 215 235
do
    echo -e -n "$i\t"
    wgrib2 -d 655 -ij $i 11 ${queue_element} | awk -F'=' '{print $2}'
    echo
done
