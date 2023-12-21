# syntax = docker/dockerfile:1.2

# The import image, used for running just the import script
FROM debian:bookworm-slim AS import

ARG COMMITBRANCH=development
ARG COMMITSHA=unspecified
ARG BUILDVER=dev

ENV BRANCH=${COMMITBRANCH}
ENV COMMIT=${COMMITSHA}
ENV VERSION=${BUILDVER}

LABEL version=${BUILDVER} code.branch=${COMMITBRANCH} code.commit=${COMMITSHA}

# Run OS updates
RUN apt-get update && apt-get upgrade -y && \
    # Install runtime deps for the script
    apt-get install -y curl jq && \
    apt-get clean && rm -rf /var/lib/apt/lists/* 

# Specify the UID/GID we want our user to have.
# In this case, use the same uid/gid as the local amb-verif user.
ENV ID=5002

# Add a user with a known uid/gid
# Create a home dir so we have a place for temporary cache dirs & etc...
RUN groupadd --gid ${ID} amb-verif && \
    useradd --shell /bin/bash --create-home --uid ${ID} --gid ${ID} amb-verif

WORKDIR /app

# Copy the scripts and metadata dirs so the import script can run
COPY ./scripts /app/scripts
COPY ./mats_metadata_and_indexes /app/mats_metadata_and_indexes

# TODO - install the cbtools directly and remove from the git repo
# See: https://docs.couchbase.com/cloud/reference/command-line-tools.html#download-and-install-the-couchbase-command-line-tools

USER amb-verif

ENTRYPOINT ["bash", "./scripts/VXingest_utilities/run-import.sh"]