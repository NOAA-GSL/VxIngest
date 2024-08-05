#!/bin/bash
set -euo pipefail
# NOTE:
# The embedded NCEPLIBS-bufr build depends on a bufr-12.0.1.tgz file that is served from the
# EMC's internal web server. This file may not always be available. If you are unable to
# build the NCEPLIBS-bufr package, you can download the bufr-12.0.1.tgz file from
# https://drive.google.com/file/d/1ZyQsJ77j9yFKJG9nR87zejOBPKFasShl/view?usp=sharing
# and explode it in a local directory, then pass the "local_test"  option to this build script.
# For example: build.sh local_test /path/to/bufr-12.0.1 (exploded test data directory)

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

local_test=false
# Check for the local_test option
if [ "${1:-}" == "local_test" ]; then
    if [ -z "${2:-}" ]; then
        echo "The local_test option requires a path to the exploded test data from the bufr-12.0.1.tgz file."
        exit 1
    fi
    bufr_test_dir=$2
    if [ ! -d ${bufr_test_dir} ]; then
        echo "The directory ${bufr_test_dir} does not exist."
        exit 1
    fi
    echo "Using the local bufr_test_dir directory: ${bufr_test_dir}"
    local_test=true
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

# NOTE: We were using the develop version for now because none of the releases support python12 yet.
# Now that a release is made that supports python 3.12, we can switch to that.
 NCEPLIBSbufr_version="12.1.0"
# Use a specific sha for now - we know that one builds
#NCEPLIBSbufr_version="0d9834838df19879d5469c4c121f81d00eb13c66"
tmp_workdir=$(mktemp -d)
cd $tmp_workdir
wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/tags/v${NCEPLIBSbufr_version}.tar.gz
#wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/${NCEPLIBSbufr_version}.zip
tar -xzf v${NCEPLIBSbufr_version}.tar.gz
#unzip ${NCEPLIBSbufr_version}.zip
cd NCEPLIBS-bufr-${NCEPLIBSbufr_version}

# Create and use a 3.12 python venv
# Check python version
pver=$(python --version | awk '{print $2}' | awk -F'.' '{print $1""$2}')
if [ ${pver} != "312" ]; then
    echo "Wrong python version - should be 3.12.x"
    exit 1
fi

# get the platform
platform=$(python -c "import sysconfig;print(sysconfig.get_platform())")
# transform the platform string to the format used by many linux (change '-' and '.' to '_' and make it lowercase)
platform=$(echo ${platform} | tr '[:upper:]' '[:lower:]' | tr '-' '_' | tr '.' '_')
# create venv
python -m venv .venv-3.12
# activate venv
. .venv-3.12/bin/activate
# add poetry to path
PATH=$PATH:${HOME}/.local/bin
# upgrade pip
pip install --upgrade pip
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
if [ "${local_test}" = true ]; then
    cmake -DCMAKE_INSTALL_PREFIX=./install -DENABLE_PYTHON=ON -DTEST_FILE_DIR=${bufr_test_dir} ..
else
    cmake -DCMAKE_INSTALL_PREFIX=./install -DENABLE_PYTHON=ON ..
fi
make -j2 VERBOSE=1
make install
ctest --verbose --output-on-failure --rerun-failed
if [ $? -ne 0 ]; then
    echo "ctest did not pass!"
#    exit 1
fi

# Now the poetry parts must be copied into the ${tmp_workdir} to enable the poetry build
# linux_x86_64 appears to want to put all this lib stuff under lib64 not lib
libdir="lib"
if [ "$platform" = "linux_x86_64" ]; then
    libdir="lib64"
fi
cd ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/${libdir}/python3.12/site-packages
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
# NOTE: THis should be unnecessary but apparently poetry doesn't yet know how to
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
