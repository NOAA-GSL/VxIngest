# Inclucing NCEPLIBS-bufr

This is a holder for building a wheel for inclusion of NCEPLIBS-bufr into this project.

## NCEPLIBS-bufr build

You must first download and build the NCEPLIBS-bufr library.
This code relies on the NCEP BUFRLIB libraries that enable reading of PREPBUFR files.
The repo is [NCEPLIBS-bufr](https://github.com/NOAA-EMC/NCEPLIBS-bufr)
This builds all the fortran stuff and the python wrappers. You do have to make sure
you have gfortran installed on the build platform, for example on my mac... brew install gfortran
You may also have to install cmake ... brew install cmake
and you can always install python 3.12 with ... brew install python@3.12
This build is following the git workflow for mac. ...

Do not build the NCEPLIBS in this directory, rather build it in its own directory and copy the
resulting library into the lib directory in this folder.

[mac workflow](https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/develop/.github/workflows/MacOS.yml)

There are other workflows i.e. linux, intel, etc.
[workflows](https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/develop/.github/workflows)

To download ...
[download](https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/heads/develop.zip)

```bash

workdir=~/something
mkdir ${workdir}

mkdir ${workdir}/NCEPLIBS
cd ${workdir}/NCEPLIBS
unzip ~/Downloads/NCEPLIBS-bufr-develop.zip
cd NCEPLIBS-bufr-develop

# Create and use a 3.12 python venv
python -V. (should show Python 3.12.2)
python -m venv .venv-3.12
. .venv-3.12/bin/activate

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
```

The python package is now installed in ${workdir}/NCEPLIBS-bufr-develop/build/install/lib/python3.12/site-packages/ncepbufr

and the required static library is now ${workdir}/NCEPLIBS-bufr-develop/build/install/lib/python3.12/site-packages/_bufrlib.cpython-312-darwin.so

These must be copied into this directory

```bash
cd .../VxIngest/NCEPLIBS-bufr
cp ${workdir}/NCEPLIBS/NCEPLIBS-bufr-develop/build/install/lib/python3.12/site-packages/_bufrlib.cpython-312-darwin.so lib
cp -a ${workdir}/NCEPLIBS/NCEPLIBS-bufr-develop/build/install/lib/python3.12/site-packages/ncepbufr/* ncepbufr

## python wheel

Now we have a static library dependency in .../VxIngest/NCEPLIBS-bufr/lib and a python package in ncepbufr. We can treat these as dependencies to this build. These will be referenced in the top level pyproject.toml
