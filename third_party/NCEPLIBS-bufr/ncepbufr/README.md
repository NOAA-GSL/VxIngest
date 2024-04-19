# Building NCEPLIBS-bufr wheel

This is a holder for building a wheel for inclusion of NCEPLIBS-bufr into this project.
The architecture of the system on which this process runs will be reflected in the wheel
that is built, as well as the version of the NCEPLIBSbufr project to which the wheel applies.

This code relies on the NCEP BUFRLIB libraries that enable reading of PREPBUFR files.
The repo is [NCEPLIBS-bufr](https://github.com/NOAA-EMC/NCEPLIBS-bufr)
This builds all the fortran stuff and the python wrappers. You do have to make sure
you have gfortran installed on the build platform, for example on my mac... brew install gfortran
You may also have to install cmake ... brew install cmake
and you can always install python 3.12 with ... brew install python@3.12

This build follows the git workflow for whatever OS and architecture the script is running on. For CI the docker container should be configured
with the necessary build tools to compile fortran, and to use cmake.

The NCEPLIBS project is built it in its own temporary directory and then copies the
python project pyproject.toml files into the python project directory that is in the temporary directory. Once the wheel is built the wheel is copied into the wheel_dist directory and the temporary directory is destroyed.

set a version in the actual script ....
i.e.
NCEPLIBSbufr_version="12.0.1"

Or to use a specific sha for now - we know that one builds

NCEPLIBSbufr_version="0d9834838df19879d5469c4c121f81d00eb13c66"

wget <https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/${NCEPLIBSbufr_version}.zip>

Or for the development head ...

wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/heads/develop.zip

Or for official releases...

wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/tags/v${version}.tar.gz

Then run the script. There are two optional parameters that go together and enable a build with local test data. The NCEPLIBS-bufr cmake uses an external ftp server which we have found to sometimes be down. You can optionally use local testdata which you can download from [here](https://drive.google.com/file/d/1ZyQsJ77j9yFKJG9nR87zejOBPKFasShl/view?usp=sharing).

The two parameters are
build.sh local_test path_to_local_test_directory

You should see the ncepbufr package listed in the pip list output.
You should see the .whl file in the dist directory
