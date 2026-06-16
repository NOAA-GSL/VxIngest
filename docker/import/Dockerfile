# syntax = docker/dockerfile:1.2

# Build Linux meta-update binaries for both amd64 and arm64.
FROM golang:1.26-trixie AS meta-update-builder

WORKDIR /src/meta_update_middleware
COPY meta_update_middleware/go.mod meta_update_middleware/go.sum ./
RUN go mod download
COPY meta_update_middleware/ ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/meta-update-linux-amd64 .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /out/meta-update-linux-arm64 .

# The import image, supporting both linux/amd64 and linux/arm64.
FROM debian:trixie-slim AS import

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

# Add a user with a known uid/gid.
# Create a home dir so we have a place for temporary cache dirs & etc...
RUN groupadd --gid ${ID} ${USERNAME} && \
    useradd --shell /bin/bash --create-home --uid ${ID} --gid ${ID} ${USERNAME}

# Install runtime deps for the import script (bash is required by run-import.sh).
RUN apt-get update && \
    apt-get install -y --no-install-recommends bash ca-certificates curl jq procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install the Couchbase dev tools (cbimport) for the target architecture.
# Available builds: linux_x86_64 (amd64) and linux_aarch64 (arm64).
ARG CBTOOLS_VERSION=8.0.1
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "${arch}" in \
    amd64) cb_arch="x86_64"  ;; \
    arm64) cb_arch="aarch64" ;; \
    *)     echo "Unsupported architecture: ${arch}"; exit 1 ;; \
    esac; \
    url="https://packages.couchbase.com/releases/${CBTOOLS_VERSION}/couchbase-server-dev-tools-${CBTOOLS_VERSION}-linux_${cb_arch}.tar.gz"; \
    mkdir -p /home/${USERNAME}/cbtools/bin; \
    curl -fsSL "${url}" | tar xz --strip-components=1 -C /home/${USERNAME}/cbtools; \
    test -x /home/${USERNAME}/cbtools/bin/cbimport; \
    chown -R ${USERNAME}:${USERNAME} /home/${USERNAME}/cbtools

# Copy the import script tree and middleware source (used by meta-update settings).
COPY ./scripts/VXingest_utilities /app/scripts/VXingest_utilities
COPY ./meta_update_middleware /app/meta_update_middleware

# Install pre-built meta-update binaries and a runtime launcher.
COPY --from=meta-update-builder /out/meta-update-linux-amd64 /app/meta_update_middleware/bin/meta-update-linux-amd64
COPY --from=meta-update-builder /out/meta-update-linux-arm64 /app/meta_update_middleware/bin/meta-update-linux-arm64
RUN printf '%s\n' \
    '#!/bin/sh' \
    'set -eu' \
    '' \
    'arch="$(uname -m)"' \
    'case "${arch}" in' \
    '    x86_64|amd64)' \
    '        exec /app/meta_update_middleware/bin/meta-update-linux-amd64 "$@"' \
    '        ;;' \
    '    aarch64|arm64)' \
    '        exec /app/meta_update_middleware/bin/meta-update-linux-arm64 "$@"' \
    '        ;;' \
    '    *)' \
    '        echo "unsupported architecture: ${arch}" >&2' \
    '        exit 1' \
    '        ;;' \
    'esac' \
    > /app/meta_update_middleware/meta-update \
    && chmod +x \
    /app/meta_update_middleware/meta-update \
    /app/meta_update_middleware/bin/meta-update-linux-amd64 \
    /app/meta_update_middleware/bin/meta-update-linux-arm64 \
    && chown -R ${USERNAME}:${USERNAME} /app/meta_update_middleware

USER ${USERNAME}

ENTRYPOINT ["bash", "./scripts/VXingest_utilities/import/run-import.sh"]
