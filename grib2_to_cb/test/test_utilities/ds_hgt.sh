#!/bin/bash
export queue_element="/opt/data/grib2_to_cb/input_files/2125214000000"
echo "DewPoint"
echo
for j in 10 20 30 40 50 150 250 350 450 550 650 750
do
    echo -e -n "$j\t"
    wgrib2 -d 619 -ij 11 $j ${queue_element} | awk -F'=' '{print $2}'
    echo
done

for i in 5 25 45 65 85 105 135 155 175 195 215 235
do
    echo -e -n "$i\t"
    wgrib2 -d 619 -ij $i 11 ${queue_element} | awk -F'=' '{print $2}'
    echo
done

echo "RH"
echo
for j in 10 20 30 40 50 150 250 350 450 550 650 750
do
    echo -e -n "$j\t"
    wgrib2 -d 620 -ij 11 $j ${queue_element} | awk -F'=' '{print $2}'
    echo
done

for i in 5 25 45 65 85 105 135 155 175 195 215 235
do
    echo -e -n "$i\t"
    wgrib2 -d 620 -ij $i 11 ${queue_element} | awk -F'=' '{print $2}'
    echo
done
