cd ${HOME}/VXingest
export PYTHONPATH=`pwd`
cd netcdf_to_cb
pytest -s test/test_metar_obs_netcdf.py::TestNetcdfObsBuilderV01::test_compare_obs_to_mysql > ~/obs-mysql-cb-comp.txt
echo "passed: $(grep passed ~/obs-mysql-cb-comp.txt)"
echo "press: $(grep press ~/obs-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "temp: $(grep temp ~/obs-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "dp: $(grep dp ~/obs-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "wd: $(grep wd ~/obs-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "wd(no None): $(grep wd ~/obs-mysql-cb-comp.txt | grep -v None | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "ws(no None): $(grep ws ~/obs-mysql-cb-comp.txt | grep -v None | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "visibility(no None): $(grep visibility ~/obs-mysql-cb-comp.txt | grep -v None | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "ceiling(no None): $(grep ceiling ~/obs-mysql-cb-comp.txt | grep -v None | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
