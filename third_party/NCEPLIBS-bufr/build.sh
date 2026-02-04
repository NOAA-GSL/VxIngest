#!/bin/bash
set -euo pipefail

# NOTE:
# The embedded NCEPLIBS-bufr build depends on a bufr-12.2.0.tgz file that is served from the
# EMC's internal web server. This file may not always be available. If you are unable to
# build the NCEPLIBS-bufr package, you can download the bufr-12.2.0.tgz file from
# https://drive.google.com/file/d/1ZyQsJ77j9yFKJG9nR87zejOBPKFasShl/view?usp=sharing
# and explode it in a local directory, then pass the "-t" option with the path to the
# exploded test data directory.

#==============================================================================
# Validate repository location
#==============================================================================
VxIngest_root_dir=$(git rev-parse --show-toplevel)
ret=$?
if [ $ret -ne 0 ]; then
    echo "This script must run from the root of the VxIngest repo."
    exit 1
fi
basename_root=$(basename ${VxIngest_root_dir})
basename_current=$(basename "$(pwd)")
if [ "${basename_root}" != ${basename_current} ]; then
    echo "This script must run from the root of the VxIngest repo."
    exit 1
fi

#==============================================================================
# Parse arguments
#==============================================================================
local_build_dir=""
bufr_test_dir=""
local_test=false
NCEPLIBSbufr_version="12.2.0"

usage() {
    echo "Usage: $0 [-l <local_build_dir>] [-t <bufr_test_dir>] [-v NCEPLIBSbufr_version]" 1>&2
    echo "  -l: Path to local build directory (if absent a tmpdir will be used)"
    echo "  -t: Path to exploded test data from the bufr-${NCEPLIBSbufr_version}.tgz file"
    echo "  -v: NCEPLIBS-bufr version (default: ${NCEPLIBSbufr_version})"
    exit 1
}

while getopts ":l:t:v:" o; do
    case "${o}" in
    l)
        local_build_dir=${OPTARG}
        [ -d ${local_build_dir} ] || usage
        echo "Using local build directory ${local_build_dir}"
        ;;
    t)
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

#==============================================================================
# Check required tools
#==============================================================================
check_required_tools() {
    for tool in gfortran cmake wget unzip tar poetry; do
        if ! command -v $tool &> /dev/null; then
            echo "You do not appear to have $tool installed. You must have $tool installed."
            exit 1
        fi
    done
}

#==============================================================================
# Check Python version and set up environment variables
#==============================================================================
setup_python_environment() {
    pver=$(python --version | awk '{print $2}' | awk -F'.' '{print $1""$2}')
    if [ ! ${pver} -ge 313 ]; then
        echo "Wrong python version - should be greater than or equal to 3.13.x"
        exit 1
    fi
    
    pyver=$(python --version | awk '{print $2}' | awk -F'.' '{print $1"."$2}')
    echo "Using python version ${pyver}"
    
    platform=$(python -c "import sysconfig;print(sysconfig.get_platform())")
    platform=$(echo ${platform} | tr '[:upper:]' '[:lower:]' | tr '-' '_' | tr '.' '_')
    
    # Export for use in other functions
    export pyver pver platform
}

#==============================================================================
# Download and extract NCEPLIBS-bufr
#==============================================================================
download_and_extract() {
    if [ -z "${local_build_dir}" ]; then
        tmp_workdir=$(mktemp -d)
    else
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
    
    # Export for use in other functions
    export tmp_workdir
}

#==============================================================================
# Create Python virtual environment and install dependencies
#==============================================================================
setup_venv() {
    python -m venv .venv-${pyver}
    . .venv-${pyver}/bin/activate
    
    PATH=$PATH:${HOME}/.local/bin
    pip install --upgrade pip
    
    trap "deactivate" EXIT
    
    pip3 install numpy meson ninja netCDF4 protobuf
}

#==============================================================================
# Build NCEPLIBS-bufr with CMake
#==============================================================================
build_nceplibs() {
    mkdir build
    cd build
    
    if [ "${local_test}" = true ]; then
        cmake -DCMAKE_INSTALL_PREFIX=./install -DENABLE_PYTHON=ON -DTEST_FILE_DIR=${bufr_test_dir} ..
    else
        cmake -DCMAKE_INSTALL_PREFIX=./install -DENABLE_PYTHON=ON -DBUILD_TESTING=OFF ..
    fi
    
    make -j2 VERBOSE=1
    make install
    
    ctest --verbose --output-on-failure --rerun-failed
    if [ $? -ne 0 ]; then
        echo "ctest did not pass!"
        exit 1
    fi
}

#==============================================================================
# Build Poetry wheel
#==============================================================================
build_wheel() {
    # Determine lib directory (linux_x86_64 uses lib64)
    libdir="lib"
    if [ "$platform" = "linux_x86_64" ]; then
        libdir="lib64"
    fi
    
    # Copy the platform specific .so file to a common name for the poetry build
    cp ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/${libdir}/python${pyver}/site-packages/_bufrlib.cpython-${pver}*.so \
       ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/${libdir}/python${pyver}/site-packages/_bufrlib.so
    
    cd ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/${libdir}/python${pyver}/site-packages
    
    # Copy poetry metadata from VxIngest repo
    cp -a ${VxIngest_root_dir}/third_party/NCEPLIBS-bufr/ncepbufr/* .
    
    rm -rf poetry.lock
    poetry build
    poetry install
    pip list
    
    # Rename the wheel (hack to work around poetry not setting build tags properly)
    dst_name_tmp=$(ls -1 dist/*.whl | sed "s/py3/py${pver}/" | sed "s/any/${platform}/")
    dst_name=$(basename ${dst_name_tmp})
    
    wheel=$(ls -1 dist/*.whl)
    cp ${wheel} ${VxIngest_root_dir}/third_party/NCEPLIBS-bufr/wheel_dist/${dst_name}
}

#==============================================================================
# Cleanup
#==============================================================================
cleanup() {
    cd ${VxIngest_root_dir}
    
    if [ -z "${local_build_dir}" ]; then
        echo "Removing temporary work directory ${tmp_workdir}"
        rm -rf ${tmp_workdir}
    else
        echo "Using local build directory ${local_build_dir} - not removing."
    fi
}

#==============================================================================
# Main execution
#==============================================================================
check_required_tools
setup_python_environment
download_and_extract
setup_venv
build_nceplibs
build_wheel
cleanup

exit 0
