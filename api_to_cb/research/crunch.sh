#!/usr/bin/env bash

prefix="$(echo $1 | echo 2022_12_26_12Z.txt | awk -F'.' '{print $1}')_"
gcsplit -q -f ${prefix} 2022_12_26_12Z.txt '/^ *254 .*$/' {*}

echo wmo_ids
grep "^ *1 .*"  $1 | awk '{print $2}' | wc -l

echo station_ids
grep "^ *3 .*"  $1 | awk '{print $2}' | wc -l

echo "stations that have a corresponding wmoid"
grep -A2 "^ *1 .*"  *.txt | awk '{print $2}' | while read l
 do
    if [[ $l =~ ^[0-9]+$ ]]
         then
             echo -n "$l "
         else
             echo $l
     fi
 done | grep -v "99999 \d* 9999" | grep -v "99999 \d*" | sed '/^[[:space:]]*$/d' | wc -l

echo "stations that have no corresponding wmoid"
grep -A2 "^ *1 .*"  $1 | awk '{print $2}' | while read l
do
    if [[ $l =~ ^[0-9]+$ ]]
        then echo -n "$l "
    else
        echo $l
    fi
done | grep -v "99999 \d* 9999" | sed '/^[[:space:]]*$/d' | wc -l

echo "wmoids that have no corresponding station"
grep -A2 "^ *1 .*"  *.txt | awk '{print $2}' | while read l
 do
    if [[ $l =~ ^[0-9]+$ ]]
    then
       echo -n "$l "
    else
       echo $l
    fi
 done  |  grep -v "99999" | grep "9* $"  | sed '/^[[:space:]]*$/d' | wc -l
exit
echo
echo
for i in $(ls -1 ${prefix}*)
do
	echo mandatory levels per station
	grep "^ *4 .*"  $i | awk '{print $2}' | wc -l

	echo
	echo significant levels per station
	grep "^ *5 .*"  $i | awk '{print $2}' | wc -l

	echo
	echo wind level per station
	grep "^ *6 .*"  $i | awk '{print $2}' | wc -l

	echo
	echo tropopause level per station
	grep "^ *7 .*"  $i | awk '{print $2}' | wc -l

	echo
	echo max wind level per station
	grep "^ *8 .*"  $i | awk '{print $2}' | wc -l

	echo
	echo surface level per station
	grep "^ *9 .*"  $i | awk '{print $2}' | wc -l
done

