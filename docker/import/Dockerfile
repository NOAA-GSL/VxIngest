# syntax = docker/dockerfile:1.2

# Build the metadata middleware with the Go version declared by the module.
FROM --platform=linux/amd64 golang:1.25-bookworm AS middleware-builder

WORKDIR /src
COPY ./meta_update_middleware /src/meta_update_middleware
RUN cd /src/meta_update_middleware && go build -o /out/meta-update .

# The import image, used for running just the import script

FROM --platform=linux/amd64 debian:bookworm-slim AS import

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
ARG USERNAME=amb-verif

# Add a user with a known uid/gid
# Create a home dir so we have a place for temporary cache dirs & etc...
RUN groupadd --gid ${ID} ${USERNAME} && \
    useradd --shell /bin/bash --create-home --uid ${ID} --gid ${ID} ${USERNAME}

# Run OS updates
RUN apt-get update && apt-get upgrade -y && \
    # Install runtime deps for the script.
    apt-get install -y curl jq procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/* 

WORKDIR /app

# Install the Couchbase dev tools (cbimport) expected by the script in $HOME/cbtools/bin.
ARG CBTOOLS_URL=https://packages.couchbase.com/releases/8.0.1/couchbase-server-dev-tools-8.0.1-linux_x86_64.tar.gz
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    if [ "${arch}" != "amd64" ]; then \
    echo "This image must be built for amd64, found: ${arch}"; \
    exit 1; \
    fi; \
    mkdir -p /home/${USERNAME}/cbtools/bin; \
    curl -fsSL "${CBTOOLS_URL}" | tar xz --strip-components=1 -C /home/${USERNAME}/cbtools; \
    test -x /home/${USERNAME}/cbtools/bin/cbimport; \
    chown -R ${USERNAME}:${USERNAME} /home/${USERNAME}/cbtools

# Copy the scripts and middleware so the import script can run.
COPY ./scripts/VXingest_utilities /app/scripts/VXingest_utilities
COPY ./meta_update_middleware /app/meta_update_middleware
COPY --from=middleware-builder /out/meta-update /app/meta_update_middleware/meta-update

# Create expected import data mount root and logs subdir.
RUN mkdir -p /opt/data_import/logs && chown -R ${USERNAME}:${USERNAME} /opt/data_import

USER ${USERNAME}

ENTRYPOINT ["bash", "./scripts/VXingest_utilities/import/run-import.sh"]
