# VXingest Utility Scripts

This directory contains operator-facing helper scripts for running ingest workflows.

## run_ingest.sh

Path: scripts/VXingest_utilities/run_ingest.sh

### What it does

run_ingest.sh executes one or more ingest job spec ids and performs the full pipeline for each job:

1. Runs VxIngest in Docker.
2. Extracts any transfer tar.gz archives written by ingest.
3. Imports JSON and JSON.GZ documents with vximporter.
4. For MODEL jobs, derives and runs CTC and SUMS jobs.
5. Runs VxMetadataUpdater when at least one job run succeeded.
6. Prints one final SUCCESS or FAILED status message.

### Usage

```bash
cd ${HOME}/VxIngest
./scripts/VXingest_utilities/run_ingest.sh JOB_SPEC_ID_1 [JOB_SPEC_ID_2 ...]
```

Examples:

```bash
cd ${HOME}/VxIngest
./scripts/VXingest_utilities/run_ingest.sh JS:METAR:MODEL:RRFS:schedule:job:V01
```

```bash
cd ${HOME}/VxIngest
./scripts/VXingest_utilities/run_ingest.sh \
  JS:METAR:MODEL:RRFS:schedule:job:V01 \
  JS:METAR:OBS:NETCDF:schedule:job:V01
```

Show built-in help:

```bash
cd ${HOME}/VxIngest
./scripts/VXingest_utilities/run_ingest.sh --help
```

### Required environment

- CREDENTIALS_FILE
  - Path to Couchbase credentials file.
  - If unset, the script uses HOME/credentials when HOME is set.
  - The run fails if the resolved credentials file does not exist.

### Optional environment

- WORKING_ROOT_DIR (default: /data-ingest/data/working)
- PUBLIC_DIR (default: /public)
- DATA_SOURCE (default: unset)
- CONTAINER_DATA_PATH (default: DATA_SOURCE when DATA_SOURCE is set)
- VXINGEST_IMAGE (default: ghcr.io/noaa-gsl/vxingest/ingest:latest)
- DOCKER_RUN_USER (default: host uid:gid)
- VXINGEST_DOCKER_USER (default: DOCKER_RUN_USER)
- LOG_LEVEL (default: INFO)
- VXIMPORTER_IMAGE (default: ghcr.io/noaa-gsl/vximporter:latest)
- VXIMPORTER_DOCKER_USER (default: DOCKER_RUN_USER)
- VXIMPORTER_WORKERS (default: 16)
- VXIMPORTER_BATCH_SIZE (default: 1000)
- VX_METADATA_UPDATER_IMAGE (default: ghcr.io/noaa-gsl/vxmetadataupdater:latest)
- VX_METADATA_UPDATER_SETTINGS (default: unset, container defaults used)
- VX_METADATA_UPDATER_DOCKER_USER (default: uid:gid of CREDENTIALS_FILE)
- VX_METADATA_UPDATER_LOCK_STALE_SECONDS (default: 7200)

### Output and logs

- Ingest logs:
  - WORKING_ROOT_DIR/logs/docker-ingest-{hostname}-{pid}-{job-id}-{timestamp}.out
- Import logs:
  - WORKING_ROOT_DIR/logs/docker-import-{hostname}-{pid}-{job-id}-{timestamp}.out
- Metrics output:
  - WORKING_ROOT_DIR/common/job_metrics
- Archived transfer tarballs:
  - WORKING_ROOT_DIR/archive/{hostname}-{original-tar-name}

### Concurrency behavior

- Temporary working directories are host- and pid-scoped.
- Cleanup runs on normal completion and also on interrupt/termination via trap handling.
- Metadata updater is guarded by an NFS-safe lock acquired using atomic mkdir at:
  - WORKING_ROOT_DIR/locks/vxmetadataupdater.lock.d
- If the lock is held by another run, metadata update is skipped for this run.
- If the lock is older than VX_METADATA_UPDATER_LOCK_STALE_SECONDS, stale-lock recovery is attempted.

### Notes

- Job ids must begin with JS:. These are the document ids from job_specification documents
  that are located in the RUNTIME collection of the vxdata bucket in the Capella database.

  #### Examples

  - JS:METAR:OBS:NETCDF:schedule:job:V01
  - JS:METAR:MODEL:HRRR_OPS:schedule:job:V01
- Temporary job directories are cleaned up automatically, including on interrupt/termination.
- When a MODEL job fails, derived CTC and SUMS jobs for that MODEL are skipped.
