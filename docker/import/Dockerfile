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

# Specify the UID/GID we want our user to have.
# In this case, use the same uid/gid as the ingest image.
ARG ID=5002
ARG USERNAME=import

# Add a user with a known uid/gid
# Create a home dir so we have a place for temporary cache dirs & etc...
RUN groupadd --gid ${ID} ${USERNAME} && \
    useradd --shell /bin/bash --create-home --uid ${ID} --gid ${ID} ${USERNAME}

# Run OS updates
RUN apt-get update && apt-get upgrade -y && \
    # Install runtime deps for the script
    apt-get install -y curl jq && \
    apt-get clean && rm -rf /var/lib/apt/lists/* 

WORKDIR /app

# Install promql
ARG PROMQL_VERSION=v0.3.0
RUN echo "$(uname -m | sed s/aarch64/arm64/)" && \
    curl -L https://github.com/nalbury/promql-cli/releases/download/${PROMQL_VERSION}/promql-${PROMQL_VERSION}-linux-$(uname -m | sed s/aarch64/arm64/).tar.gz | tar xz --directory /usr/local/bin

# Install the couchbase tools
ARG CB_VERSION=7.2.2
RUN curl -L https://packages.couchbase.com/releases/${CB_VERSION}/couchbase-server-tools_${CB_VERSION}-linux_$(uname -m).tar.gz | tar xz --directory /usr/local

# Copy the scripts and metadata dirs so the import script can run
COPY ./scripts /app/scripts
COPY ./mats_metadata_and_indexes /app/mats_metadata_and_indexes

# TODO - install the cbtools directly and remove from the git repo
# See: https://docs.couchbase.com/cloud/reference/command-line-tools.html#download-and-install-the-couchbase-command-line-tools

USER ${USERNAME}

ENTRYPOINT ["bash", "./scripts/VXingest_utilities/run-import.sh"]
