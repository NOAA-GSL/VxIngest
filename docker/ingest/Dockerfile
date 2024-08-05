# syntax = docker/dockerfile:1.2

# NOTE - avoid installing python packages via apt - they install an alternate version of Python

# The eccodes image, used for building the eccodes library against the correct Python version
FROM python:3.11-slim-bookworm AS eccodes

ARG ECCODES_VER=2.32.1

RUN apt-get update && \
    apt-get install -y curl wget && \
    apt-get install -y build-essential libssl-dev libnetcdff-dev libopenjp2-7-dev gfortran make unzip git cmake && \
    mkdir /build && cd /build && \
    wget https://confluence.ecmwf.int/download/attachments/45757960/eccodes-${ECCODES_VER}-Source.tar.gz && \
    tar -xzf  eccodes-${ECCODES_VER}-Source.tar.gz && \
    mkdir eccodes-build && cd eccodes-build && \
    mkdir /eccodes && \
    # Default install location is /usr/local/{bin,include,lib}, use the CMAKE prefix to change to /eccodes
    # Note you'll need to set some bashrc variables (ECCODES_DEFINITION_PATH & ECCODES_DIR) in subsequent images
    # cmake -DCMAKE_INSTALL_PREFIX=/eccodes -DBUILD_SHARED_LIBS=ON -DENABLE_JPG=ON ../eccodes-${ECCODES_VER}-Source && \
    cmake -DCMAKE_INSTALL_PREFIX=/eccodes ../eccodes-${ECCODES_VER}-Source && \
    # Use nproc to get the number of available cores - use all but 1 core for compilation
    make -j$(( $(nproc)-1 )) && \
    make install


# The builder image, used for building the virtual environment
FROM python:3.11-slim-bookworm AS builder

COPY --from=eccodes /eccodes /usr/local

# Make sure eccodes knows where to find its definitions
ENV ECCODES_DEFINITION_PATH=/usr/local/share/eccodes/definitions/ \
    ECCODES_DIR=/usr/local/

RUN apt-get update && \
    # Runtime deps for the native eccodes library
    apt-get install -y libopenjp2-7 libaec0 && \
    # cftime build deps
    apt-get install -y build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/* 

# Make sure the python tooling is up-to-date for when it's copied into the prod layer
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir poetry

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

# Install just the runtime dependencies, no dev tooling
COPY pyproject.toml poetry.lock ./
RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR


# The dev image, used for testing
FROM builder AS dev

# Use like the following:
# docker build --target=dev -f Dockerfile -t vxingest:dev .
# docker run --rm --mount type=bind,src=$(pwd)/tmp/test-data/opt/data,dst=/opt/data -it vxingest:dev bash
# poetry run pytest tests

# Install the app and dev dependencies so we can run tests & tooling
COPY . /app
RUN poetry install


# The runtime image, used for running just the application with its dependencies
FROM python:3.11-slim-bookworm AS prod

ARG COMMITBRANCH=development
ARG COMMITSHA=unspecified
ARG BUILDVER=dev

ENV BRANCH=${COMMITBRANCH}
ENV COMMIT=${COMMITSHA}
ENV VERSION=${BUILDVER}

LABEL version=${BUILDVER} code.branch=${COMMITBRANCH} code.commit=${COMMITSHA}

# Update the image's built in Python tooling before we activate the virtual environment
RUN pip install --no-cache-dir --upgrade pip setuptools

# Activate the virtual environment
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

# Copy in dependencies
COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY --from=eccodes /eccodes /usr/local

# Make sure eccodes knows where to find its definitions
ENV ECCODES_DEFINITION_PATH=/usr/local/share/eccodes/definitions/ \
    ECCODES_DIR=/usr/local/

WORKDIR /app

# Specify the UID/GID we want our user to have.
# In this case, use the same uid/gid as the import image.
ARG ID=5002
ARG USERNAME=ingest

# Add a user with a known uid/gid
# Create a home dir so we have a place for temporary cache dirs & etc...
RUN groupadd --gid ${ID} ${USERNAME} && \
    useradd --shell /bin/bash --create-home --uid ${ID} --gid ${ID} ${USERNAME}

# Run OS updates
RUN apt-get update && apt-get upgrade -y && \
    # Install runtime deps for the native eccodes library
    apt-get install -y libopenjp2-7 libaec0 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* 

# Copy just the vxingest app
COPY ./src/ /app/

USER ${USERNAME}

ENTRYPOINT ["python", "-m", "vxingest.main"]
