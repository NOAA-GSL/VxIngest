# Specify the base image -- here we're using ne that bundles the OpenJDK version of Java 8 on top of a generic Debian Linux OS
# syntax = docker/dockerfile:1.2
FROM python:3.11-slim-bullseye AS prod

ARG COMMITBRANCH=development
ARG COMMITSHA
ARG BUILDVER=dev

ENV BRANCH=${COMMITBRANCH}
ENV COMMIT=${COMMITSHA}
ENV VERSION=${BUILDVER}

# Identify the maintainer of an image
LABEL maintainer="randy.pierce@noaa.gov"

LABEL version=${BUILDVER} code.branch=${COMMITBRANCH} code.commit=${COMMITSHA}

# update and create an amb-verif user
# need to install wget to get the libssl1.1_1.1.1f-1ubuntu2.19_amd64.deb file
RUN apt-get update && \
        apt-get upgrade -y && \
        useradd -d /home/amb-verif -m -s /bin/bash amb-verif && \
        apt-get install -y jq && \
        apt-get install -y curl && \
        apt-get install -y wget && \
        apt-get install python3-dev python3-pip python3-setuptools build-essential libssl-dev libnetcdff-dev libopenjp2-7-dev gfortran make unzip git cmake -y && \
        cd && mkdir build && cd build && \
        wget https://confluence.ecmwf.int/download/attachments/45757960/eccodes-2.32.1-Source.tar.gz && \
        tar -xzf  eccodes-2.32.1-Source.tar.gz && \
        mkdir build ; cd build && \
        cmake -DCMAKE_INSTALL_PREFIX=/usr/src/eccodes -DENABLE_EXTRA_TESTS=ON ../eccodes-2.32.1-Source && \
        make && \
#        ctest && \
        make install && \
        cp -r /usr/src/eccodes/bin/* /usr/bin/ && \
        cp /usr/src/eccodes/lib/libeccodes.so /usr/lib && \
        cp /usr/src/eccodes/include/* /usr/include/ && \
        echo 'export ECCODES_DIR=/usr/src/eccodes' >> ~/.bashrc && \
        echo 'export ECCODES_DEFINITION_PATH=/usr/src/eccodes/share/eccodes/definitions' >> ~/.bashrc && \
        pip install couchbase --no-binary couchbase && \
        # cleanup
        apt-get autoremove && apt-get clean && \
        rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
        apt-get remove -y wget && \
        apt-get remove -y wget libssl-dev libnetcdff-dev libopenjp2-7-dev gfortran make unzip git cmake build-essential python3-dev python3-pip python3-setuptools && \
        cd && rm -rf source_builds

COPY . /home/amb-verif/VxIngest

#Set the working directory to be used when the docker gets run
WORKDIR /home/amb-verif/VxIngest
#RUN cd /home/amb-verif/VxIngest && pip install --upgrade pip setuptools wheel && python -m pip install couchbase==4.1.6 --no-binary couchbase && \
RUN python -m pip install -r requirements.txt

# clean up potentially checked in pytest cache
RUN rm -rf /home/amb-verif/.pytest_cache && chown -R amb-verif /home/amb-verif/VxIngest

USER amb-verif

# create a dev target that has some utilities for debugging
FROM python:3.11-slim-bullseye AS dev
COPY --from=prod / /
RUN apt-get update && \
        apt-get upgrade -y && \
        apt-get install -y procps && \
        apt-get install -y vim && \
        apt-get install -y iputils-ping && \
        rm -rf /var/lib/apt/lists/*

USER amb-verif

#Set the working directory to be used when the docker gets run
WORKDIR /home/amb-verif/VxIngest

# make the default the prod target
FROM prod
