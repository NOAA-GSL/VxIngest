#!/bin/bash
set -euo pipefail
# NOTE:
# The embedded NCEPLIBS-bufr build depends on a bufr-12.1.0.tgz file that is served from the
# EMC's internal web server. This file may not always be available. If you are unable to
# build the NCEPLIBS-bufr package, you can download the bufr-12.1.0.tgz file from
# https://drive.google.com/file/d/1ZyQsJ77j9yFKJG9nR87zejOBPKFasShl/view?usp=sharing
# and explode it in a local directory, then pass the "local_test"  option to this build script.
# For example: build.sh local_test /path/to/bufr-12.1.0 (exploded test data directory)

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
local_build_dir=""
bufr_test_dir=""
usage() {
    echo "Usage: $0 [-l <local_test_dir>] [-b <local_build_dir>] [-v NCEPLIBSbufr_version]" 1>&2
    exit 1
    echo "The local_test option requires a path to the exploded test data from the bufr-12.2.0.tgz file."
    echo "The local_build option requires a path to a local build directory. If absent a tmdir will be used"
    exit 1
}

local_test=false
NCEPLIBSbufr_version="12.2.0"
while getopts ":l:b:v:" o; do
    case "${o}" in
        l)
            local_build_dir=${OPTARG}
            [ -d ${local_build_dir} ] || usage
            echo "Using local build directory ${local_build_dir}"
        ;;
        b)
            bufr_test_dir=${OPTARG}
            [ -d ${bufr_test_dir} ] || usage
            local_test=true
            echo "Using local test data directory ${bufr_test_dir}"
        ;;
        v)
            NCEPLIBSbufr_version=${OPTARG}
            echo "Using NCEPLIBSbufr version ${NCEPLIBSbufr_version}"
        ;;
        *)
            usage
        ;;
    esac
done
shift $((OPTIND - 1))

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

NCEPLIBSbufr_version="12.2.0"
if [ -z "${local_build_dir}" ]; then
    # create a temporary directory for the build
    tmp_workdir=$(mktemp -d)
else
    # use the local build directory
    tmp_workdir=${local_build_dir}
fi
cd $tmp_workdir
wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/tags/v${NCEPLIBSbufr_version}.tar.gz
if [ $? -ne 0 ]; then
    echo "Unable to download NCEPLIBS-bufr version ${NCEPLIBSbufr_version}. Exiting."
    exit 1
fi
tar -xzf v${NCEPLIBSbufr_version}.tar.gz
cd NCEPLIBS-bufr-${NCEPLIBSbufr_version}

# Create and use a python venv
# Check python version
pver=$(python --version | awk '{print $2}' | awk -F'.' '{print $1""$2}')
if [ ! ${pver} -ge 313 ]; then
    echo "Wrong python version - should be greater than or equal to 3.11.x"
    exit 1
fi
# capture the python version
pyver=$(python --version | awk '{print $2}' | awk -F'.' '{print $1"."$2}')
echo "Using python version ${pyver}"
# get the platform
platform=$(python -c "import sysconfig;print(sysconfig.get_platform())")
# transform the platform string to the format used by many linux (change '-' and '.' to '_' and make it lowercase)
platform=$(echo ${platform} | tr '[:upper:]' '[:lower:]' | tr '-' '_' | tr '.' '_')
# create venv
python -m venv .venv-${pyver}
# activate venv
. .venv-${pyver}/bin/activate
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
    cmake -DCMAKE_INSTALL_PREFIX=./install -DENABLE_PYTHON=ON -DBUILD_TESTING=OFF ..
fi
make -j2 VERBOSE=1
make install
#echo "Running tests..."
ctest --verbose --output-on-failure --rerun-failed
if [ $? -ne 0 ]; then
    echo "ctest did not pass!"
    exit 1
fi
# Now the poetry parts must be copied into the ${tmp_workdir} to enable the poetry build
# linux_x86_64 appears to want to put all this lib stuff under lib64 not lib
libdir="lib"
if [ "$platform" = "linux_x86_64" ]; then
    libdir="lib64"
fi
cd ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/${libdir}/python${pyver}/site-packages
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
if [ -z "${local_build_dir}" ]; then
    # remove the temporary work directory
    echo "Removing temporary work directory ${tmp_workdir}"
    rm -rf ${tmp_workdir}
else
    echo "Using local build directory ${local_build_dir} - not removing."
fi
exit 0
