#!/bin/bash
VxIngest_root_dir=$(git rev-parse --show-toplevel)
ret=$?
if [ $ret -ne 0 ]; then
    echo "This script must run from the root of the VxIngest repo."
    exit 1
fi
basename_root=$(basename ${VxIngest_root_dir})
basename_current=$(basename $(pwd))
if [ "${basename_root}" != ${basename_current} ]; then
    echo "This script must run from the root of the VxIngest repo."
    exit 1
fi

# do we have gfortran?
gfortran -v
if [ $? -ne 0 ]; then
    echo "You do not appear to have gfortran installed. You must have gfortran installed."
    exit 1
fi

cmake --version
if [ $? -ne 0 ]; then
    echo "You do not appear to have cmake installed. You must have cmake installed."
    exit 1
fi


wget -V
if [ $? -ne 0 ]; then
    echo "You do not appear to have wget installed. You must have wget installed."
    exit 1
fi


unzip -v
if [ $? -ne 0 ]; then
    echo "You do not appear to have unzip installed. You must have unzip installed."
    exit 1
fi


tar --version
if [ $? -ne 0 ]; then
    echo "You do not appear to have tar installed. You must have tar installed."
    exit 1
fi


# NOTE: We are using the develop version for now because none of the releases support python12 yet.
# When a release is made that supports python 3.12, we can switch to that. FIXME
#NCEPLIBSbufr_version="12.0.1"
NCEPLIBSbufr_version="develop"
tmp_workdir=$(mktemp -d)
cd $tmp_workdir
#wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/tags/v${NCEPLIBSbufr_version}.tar.gz
wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/heads/develop.zip
#tar -xzf v${NCEPLIBSbufr_version}.tar.gz
unzip develop.zip
cd NCEPLIBS-bufr-${NCEPLIBSbufr_version}


# Create and use a 3.12 python venv
# Check python version
pver=$(python --version | awk '{print $2}' | awk -F'.' '{print $1"_"$2}')
if [ ${pver} != "3_12" ]; then
    echo "Wrong python version - should be 3.12.x";
    exit 1
fi

# get the platform
platform=$(python -c "import sysconfig;print(sysconfig.get_platform())")

# create venv
python -m venv .venv-3.12
# activate venv
. .venv-3.12/bin/activate
# set a trap to deactivate when the script exits
trap "deactivate" EXIT

# install python dependencies for build
pip3 install numpy
pip3 install meson
pip3 install ninja
pip3 install netCDF4
pip3 install protobuf

mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=./install -DENABLE_PYTHON=ON ..
make -j2 VERBOSE=1
make install
ctest --verbose --output-on-failure --rerun-failed
if [ $? -ne 0 ]
then
    echo "ctest did not pass!"
#    exit 1
fi

# Now the poetry parts must be copied into the ${tmp_workdir} to enable the poetry build
cd ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/lib/python3.12/site-packages
cp -a ${VxIngest_root_dir}/third_party/NCEPLIBS-bufr/ncepbufr/* .

# check for poetry
poetry -V
if [ $? -ne 0 ]; then
    echo "You do not appear to have poetry installed. You must have poetry installed. See https://python-poetry.org/docs/."
    exit 1
fi

rm -rf poetry.lock
poetry build
poetry install
pip list
# You should see the ncepbufr package listed in the pip list output.
# You should see the .whl file in the dist directory
# NOTE: THis should be unnecessary but apprently poetry doesn't yet know how to
# specify the build tags properly.

# rename the wheel NOTE: This is a hack to get around the fact that poetry doesn't
# seem to know how to specify the build tags properly. FIXME when it can
dst_name_tmp=$(ls -1 dist/*.whl | sed "s/py3/py${pver}/" | sed "s/any/${platform}/")
dst_name=$(basename ${dst_name_tmp})
# copy the wheel to the dist directory
wheel=$(ls -1 dist/*.whl)
cp ${wheel} ${VxIngest_root_dir}/third_party/NCEPLIBS-bufr/wheel_dist/${dst_name}
# deactivate the venv
#deactivate
cd ${VxIngest_root_dir}
rm -rf ${tmp_workdir}
exit 0