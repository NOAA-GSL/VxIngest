#!/usr/bin/env sh
wmoid=$1

echo "data for ${wmoid}"
echo
echo "data from adpupa data dump"
awk "/SID *${wmoid}/,/END/" /opt/data/prepbufr_to_cb/test_artifacts/adpupa-verbose-242130000.txt | awk '{print $3" "$4}' | awk '/SID/||/TYP/||/POB/{if($2%10==0 || $2==54161)flag=1}/PRSLEVEL/{flag=0}flag' | egrep "SID|TYP|POB|PPC|PQM|ZOB|ZPC|ZQM|TOB|TPC|TQM|TDO|QOB|QPC|QQM|DDO|FFO|DFP|DFQ" | sed 's/\(POB.*\)/\n\1/g' > "/opt/data/prepbufr_to_cb/test_artifacts/${wmoid}_mandatory_values.txt"
python ${HOME}/VxIngest/tests/vxingest/utilities/get_data_for_raobs_from_adpupa_dump.py ${wmoid}
echo
echo "data from CB"
python ${HOME}/VxIngest/tests/vxingest/utilities/get_data_for_raobs_from_cb.py ${wmoid}
echo
echo "data from mysql"
mysql --defaults-file=~/wolphin.cnf -A -B  --execute "select press,z,t,dp,rh,wd,ws from ruc_ua_pb.RAOB where date = '2024-07-31' and hour = 0 and wmoid = ${wmoid} and press in (1000,850,700,600,500,400,300,250,200,150,100,70,50,30,20) order by press desc;"
