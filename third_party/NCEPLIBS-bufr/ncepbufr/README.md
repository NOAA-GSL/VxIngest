# Inclucing NCEPLIBS-bufr

This is a holder for building a wheel for inclusion of NCEPLIBS-bufr into this project.
The architecture of the system on which this process runs will be reflected in the wheel
that is built, as well as the version of the NCEPLIBSbufr project to which the wheel applies.

## NCEPLIBS-bufr build manual steps

These steps are what a person would do to create the wheel manually. All of these steps are
encapsulated in a build_wheel.sh script, but this is an attempt to document what the
script needs to do.

### Steps

You must first download and build the NCEPLIBS-bufr library.
This code relies on the NCEP BUFRLIB libraries that enable reading of PREPBUFR files.
The repo is [NCEPLIBS-bufr](https://github.com/NOAA-EMC/NCEPLIBS-bufr)
This builds all the fortran stuff and the python wrappers. You do have to make sure
you have gfortran installed on the build platform, for example on my mac... brew install gfortran
You may also have to install cmake ... brew install cmake
and you can always install python 3.12 with ... brew install python@3.12
This build is following the git workflow for mac. For CI the docker container should be configured
with the necessary build tools to compile fortran, and to use cmake.

The NCEPLIBS project should build it in its own temporary directory and then copy the
resulting library and the associated python project pyproject.toml files into the python
project directory that is in the temporary directory. Once the wheel is built the wheel is
copied into the wheel_dist directory and the temporary directory is destroyed.

For actually building the NCEPLIBSbufr code there are several workflows based on architecture.

This is the mac workflow.
[mac workflow](https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/develop/.github/workflows/MacOS.yml)

There are other workflows i.e. linux, intel, etc.
[workflows](https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/develop/.github/workflows)

To download ...
[download](https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/heads/develop.zip)
[download](https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/tags/v${version}.tar.gz)

The following portion of the script builds the NCEPLIBSbufr and the python wrapper and runs tests

```bash
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

# set arch and version"
arch=$(uname -m)
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
if [ $(python --version | awk '{print $2}' | awk -F'.' '{print $1"."$2}') != "3.12" ]; then
    echo "Wrong python version - should be 3.12.x";
    exit 1
fi
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
    exit 1
fi
```

Make sure the tests passed!

## Copy Python Package Artifacts and Static Library to Project Area

The python package is now installed in ${tmp_workdir}/NCEPLIBS/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/lib/python3.12/site-packages/ncepbufr
and the required static library is now ${tmp_workdir}/NCEPLIBS/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/lib/python3.12/site-packages/_bufrlib.cpython-312-darwin.so

Now the poetry parts must be copied into the ${tmp_workdir} to enable the poetry build

```bash
cd ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/lib/python3.12/site-packages
cp -a ${VxIngest_root_dir}/third_party/NCEPLIBS-bufr/ncepbufr/* .
```

## Build Python Wheel

Now we have a static library dependency in ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/lib and a python package in ncepbufr. We can treat these as dependencies to this build. These will be referenced in the top level pyproject.toml

You must have poetry installed. See [poetry](https://python-poetry.org/docs/).

```bash
poetry -V
if [ $? -ne 0 ]; then
    echo "You do not appear to have poetry installed. You must have poetry installed. See [poetry](https://python-poetry.org/docs/)."
    exit 1
fi

cd ${tmp_workdir}/NCEPLIBS-bufr-${NCEPLIBSbufr_version}/build/install/lib/python3.12/site-packages
rm -rf poetry.lock
poetry env remove
poetry build
poetry install
pip list
# You should see the ncepbufr package listed in the pip list output.
# You should see the .whl file in the dist directory
# NOTE: THis should be unnecessary but apprently poetry doesn't yet know how to
# specify the build tags properly.

# rename the wheel NOTE: This is a hack to get around the fact that poetry doesn't
# seem to know how to specify the build tags properly. FIXME when it can
dst_name=$(ls -1 dist/*.whl | sed "s/py3/py${pver}/" | sed "s/any/${arch}/")
# copy the wheel to the dist directory
cp dist/*.whl ${VxIngest_root_dir}/third_party/NCEPLIBS-bufr/wheel_dist/${dst_name}
# deactivate the venv
deactivate
cd ${VxIngest_root_dir}
rm -rf ${tmp_workdir}
exit 0
```
