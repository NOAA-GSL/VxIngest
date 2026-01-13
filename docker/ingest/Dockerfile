# syntax = docker/dockerfile:1.2

# NOTE - avoid installing python packages via apt - they install an alternate version of Python

# The builder image, used for building the virtual environment
FROM python:3.13-slim-trixie AS builder

RUN apt-get update && \
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
COPY third_party/NCEPLIBS-bufr/wheel_dist ./third_party/NCEPLIBS-bufr/wheel_dist
RUN ls && poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR


# The dev image, used for testing
FROM builder AS dev

# Use like the following:
# docker build --target=dev -f Dockerfile -t vxingest:dev .
# docker run --rm --mount type=bind,src=$(pwd)/tmp/test-data/opt/data,dst=/opt/data -it vxingest:dev bash
# poetry run pytest tests

# Install the app and dev dependencies so we can run tests & tooling
COPY . /app
RUN poetry install

# Activate the virtual environment
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"


# The runtime image, used for running just the application with its dependencies
FROM python:3.13-slim-trixie AS prod

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
    apt-get clean && rm -rf /var/lib/apt/lists/* 

# Copy just the vxingest app
COPY ./src/ /app/

USER ${USERNAME}

ENTRYPOINT ["python", "-m", "vxingest.main"]
