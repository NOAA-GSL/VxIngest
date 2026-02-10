# syntax = docker/dockerfile:1.2

# NOTE - avoid installing python packages via apt - they install an alternate version of Python

# First, build the application in the `/app` directory.
# See `Dockerfile` for details.
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Omit development dependencies
ENV UV_NO_DEV=1

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Copy in our vendored dependencies
COPY third_party/NCEPLIBS-bufr/wheel_dist ./third_party/NCEPLIBS-bufr/wheel_dist

# Set up Python environment
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Install the app
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

# The dev image, used for testing
FROM builder AS dev
# Use like the following:
# docker build --target=dev -f Dockerfile -t vxingest:dev .
# docker run --rm --mount type=bind,src=$(pwd)/tmp/test-data/opt/data,dst=/opt/data -it vxingest:dev bash
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
FROM python:3.14-slim-trixie AS prod
# It is important to use the image that matches the builder, as the path to the
# Python executable must be the same, e.g., using `python:3.11-slim-bookworm`
# will fail.

ARG COMMITBRANCH=development
ARG COMMITSHA=unspecified
ARG BUILDVER=dev

ENV BRANCH=${COMMITBRANCH}
ENV COMMIT=${COMMITSHA}
ENV VERSION=${BUILDVER}

LABEL version=${BUILDVER} code.branch=${COMMITBRANCH} code.commit=${COMMITSHA}

# Setup a non-root user with a well known UID/GID
# In this case, use the same uid/gid as the import image.
# Create the user with a home dir so we have a place for temporary cache dirs & etc...
ARG ID=5002
ARG USERNAME=ingest
RUN groupadd --gid ${ID} ${USERNAME} && \
    useradd --shell /bin/bash --create-home --uid ${ID} --gid ${ID} ${USERNAME}

# Update the image's built in Python tooling before we activate the virtual environment
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Run OS updates
RUN apt-get update && apt-get upgrade -y && \
    apt-get clean && rm -rf /var/lib/apt/lists/* 

# Copy the application from the builder
COPY --from=builder --chown=ingest:ingest /app /app

WORKDIR /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Use the non-root user to run the application
USER ${USERNAME}

# Run the default ingest python entrypoint defined in pyproject.toml
ENTRYPOINT ["ingest"]
