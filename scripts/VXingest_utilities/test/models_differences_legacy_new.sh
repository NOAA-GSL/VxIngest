cd ${HOME}/VXingest
export PYTHONPATH=`pwd`
cd netcdf_to_cb
pytest -s test/test_metar_obs_netcdf.py::TestNetcdfObsBuilderV01::test_compare_model_to_mysql > ~/model-mysql-cb-comp.txt
echo "passed: $(grep passed ~/model-mysql-cb-comp.txt)"
echo "press: $(grep press ~/model-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "temp: $(grep temp ~/model-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "dp: $(grep dp ~/model-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "rh: $(grep rh ~/model-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "wd: $(grep wd ~/model-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "ws: $(grep ws ~/model-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "visibility: $(grep visibility ~/model-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "visibility(no None): $(grep visibility ~/model-mysql-cb-comp.txt |grep -v None | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "ceiling: $(grep ceiling ~/model-mysql-cb-comp.txt | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
echo "ceiling(no None): $(grep ceiling ~/model-mysql-cb-comp.txt |grep -v None | awk 'function abs(v) {return v < 0 ? -v : v} BEGIN{a=0}{if (abs($4)>0+a) a=abs($4)} END{print a}')"
