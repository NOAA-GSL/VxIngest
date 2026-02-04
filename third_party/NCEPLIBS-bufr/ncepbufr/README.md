# Building NCEPLIBS-bufr wheel

This is a holder for building a wheel for inclusion of NCEPLIBS-bufr into this project.
The architecture of the system on which this process runs will be reflected in the wheel
that is built, as well as the version of the NCEPLIBS-bufr project to which the wheel applies.

## Overview

This code relies on the NCEP BUFRLIB libraries that enable reading of PREPBUFR files.
The repo is [NCEPLIBS-bufr](https://github.com/NOAA-EMC/NCEPLIBS-bufr).

This builds all the Fortran code and the Python wrappers.

## Prerequisites

You need to ensure the following tools are installed on the build platform:

- **gfortran**: `brew install gfortran` (macOS)
- **cmake**: `brew install cmake`
- **Python 3.12**: `brew install python@3.12`

For CI, the Docker container should be configured with the necessary build tools to compile Fortran and to use CMake.

## Build Process

This build follows the git workflow for whatever OS and architecture the script is running on.

The NCEPLIBS project is built in its own temporary directory, then the Python project `pyproject.toml` files are copied into the Python project directory within the temporary directory. Once the wheel is built, it is copied into the `wheel_dist` directory and the temporary directory is destroyed.

### Version Configuration

Set a version in the actual script:

```bash
NCEPLIBSbufr_version="12.0.1"
```

Or to use a specific SHA (we know this one builds):

```bash
NCEPLIBSbufr_version="0d9834838df19879d5469c4c121f81d00eb13c66"
```

### Download Options

For a specific version:

```bash
wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/${NCEPLIBSbufr_version}.zip
```

For the development head:

```bash
wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/heads/develop.zip
```

For official releases:

```bash
wget https://github.com/NOAA-EMC/NCEPLIBS-bufr/archive/refs/tags/v${version}.tar.gz
```

## Running the Build Script

Run the script with:

```bash
./build.sh
```

### Optional: Local Test Data

There are two optional parameters that go together and enable a build with local test data. The NCEPLIBS-bufr CMake uses an external FTP server which we have found to sometimes be down.

You can optionally use local test data which you can download from [here](https://drive.google.com/file/d/1ZyQsJ77j9yFKJG9nR87zejOBPKFasShl/view?usp=sharing).

The two parameters are:

```bash
./build.sh -t path_to_local_test_directory
```

## Verification

After the build completes:

- You should see the `ncepbufr` package listed in the `pip list` output
- You should see the `.whl` file in the `dist` directory
