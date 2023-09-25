# Specify the base image -- here we're using ne that bundles the OpenJDK version of Java 8 on top of a generic Debian Linux OS
# syntax = docker/dockerfile:1.2
FROM python:3.11.5-slim-bookworm AS prod

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
        wget http://nz2.archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2.19_amd64.deb && \
        dpkg -i libssl1.1_1.1.1f-1ubuntu2.19_amd64.deb && \
        apt-get remove -y wget

COPY . /home/amb-verif/VxIngest

#Set the working directory to be used when the docker gets run
WORKDIR /home/amb-verif/VxIngest

RUN cd /home/amb-verif/VxIngest && python -m pip install -r requirements.txt

# clean up potentially checked in pytest cache
RUN rm -rf /home/amb-verif/.pytest_cache && chown -R amb-verif /home/amb-verif/VxIngest

USER amb-verif

# create a dev target that has some utilities for debugging
FROM python:3.11.5-slim-bookworm AS dev
COPY --from=prod / /
RUN apt-get update && \
        apt-get upgrade -y && \
        apt-get install -y procps && \
        apt-get install -y vim

# make the default the prod target
FROM prod
RUN cd /home/amb-verif/VxIngest