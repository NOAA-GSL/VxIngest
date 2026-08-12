# syntax = docker/dockerfile:1.19

# NOTE - avoid installing python packages via apt - they install an alternate version of Python

ARG CHAINGUARD_PYTHON_BUILDER_TAG=latest-dev
ARG CHAINGUARD_PYTHON_RUNTIME_TAG=latest

# First, build the application in the `/app` directory.
# See `Dockerfile` for details.
FROM cgr.dev/chainguard/python:${CHAINGUARD_PYTHON_BUILDER_TAG} AS builder
USER root

# Install uv into the build image.
RUN python -m pip install --no-cache-dir uv

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
ENV UV_CACHE_DIR=/root/.cache/uv

# Omit development dependencies
ENV UV_NO_DEV=1

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0
ENV UV_PYTHON=python

WORKDIR /app

# Copy in our vendored dependencies
COPY third_party/NCEPLIBS-bufr/wheel_dist ./third_party/NCEPLIBS-bufr/wheel_dist

# Set up Python environment
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Install the app, excluding middleware that belongs only to the import image.
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

# The dev image, used for testing
FROM builder AS dev
# Use like the following:
# docker build --target=dev -f Dockerfile -t vxingest:dev .
# docker run --rm --mount type=bind,src=$(pwd)/tmp/test-data/opt/data,dst=/opt/data -it vxingest:dev sh
# uv run pytest tests

# Have uv install dev dependencies
ENV UV_NO_DEV=0

# Set up Python environment
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# The runtime image, used for running just the application with its dependencies
FROM cgr.dev/chainguard/python:${CHAINGUARD_PYTHON_RUNTIME_TAG} AS prod
# Override both tags together if you need strict Python version alignment.

ARG COMMITBRANCH=development
ARG COMMITSHA=unspecified
ARG BUILDVER=dev

ENV BRANCH=${COMMITBRANCH}
ENV COMMIT=${COMMITSHA}
ENV VERSION=${BUILDVER}

LABEL version=${BUILDVER} code.branch=${COMMITBRANCH} code.commit=${COMMITSHA}

# Copy the application from the builder
COPY --from=builder --chown=65532:65532 /app /app

WORKDIR /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Chainguard's unprivileged runtime user.
USER 65532

# Run the default ingest python entrypoint defined in pyproject.toml
ENTRYPOINT ["ingest"]
