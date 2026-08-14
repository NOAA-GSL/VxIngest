#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Script: run_job.sh
# Description: Submits and processes a VxIngest job with Docker and handles job
#              document import. Requires CREDENTIALS_FILE env var.
# Usage: ./run_job.sh <job_id>
# ------------------------------------------------------------------------------

if [ -z "${BASH_VERSION:-}" ]; then
    echo "Error: run_job.sh must be run with bash. Use: bash $0 <job_id> or ./$0 <job_id>"
    exit 1
fi
if shopt -qo posix; then
    echo "Error: run_job.sh must be run with bash, not sh. Use: bash $0 <job_id> or ./$0 <job_id>"
    exit 1
fi

set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 <job_id>

Runs a VxIngest job with Docker, extracts all generated transfer archives,
and imports every JSON or JSON.GZ document file with vximporter.

Required environment:
CREDENTIALS_FILE: Host path to the Couchbase credentials file.

Optional environment:
WORKING_ROOT_DIR: Host working directory mounted into ingest as /opt/data. Default: /data-ingest/data/working
PUBLIC_DIR: Host public directory mounted into ingest as /public. Default: /public
DATA_SOURCE: Host raw-data directory to mount read-only into ingest. Default: unset; no additional raw-data mount.
CONTAINER_DATA_PATH: Container path for DATA_SOURCE. Default: same as DATA_SOURCE when DATA_SOURCE is set.
VXINGEST_IMAGE: VxIngest image. Default: ghcr.io/noaa-gsl/vxingest/ingest:latest
LOG_LEVEL: VxIngest log level. One of DEBUG, INFO, WARNING, ERROR, or CRITICAL. Default: INFO
VXIMPORTER_IMAGE: vximporter image. Default: ghcr.io/noaa-gsl/vximporter:latest
VXIMPORTER_WORKERS: vximporter worker count. Default: 16
VXIMPORTER_BATCH_SIZE: vximporter batch size. Default: 1000
EOF
}

# Show usage when requested
if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

# Ensure exactly one argument (job_id) is provided
if [ "$#" -ne 1 ]; then
    usage
    exit 1
fi

# Check that the CREDENTIALS_FILE environment variable is set
if [ -z "${CREDENTIALS_FILE:-}" ]; then
    echo "Error: CREDENTIALS_FILE environment variable is not set."
    exit 1
fi

validate_tar_paths() {
    local tar_file="$1"
    local unsafe_entry

    tar -tzf "${tar_file}" >/dev/null
    if unsafe_entry="$(tar -tzf "${tar_file}" | awk '
        /(^\/)|(^\.\.$)|(^\.\.\/)|(\/\.\.\/)|(\/\.\.$)/ { print; found=1; exit }
        END { exit found ? 0 : 1 }
    ')"; then
        echo "Error: Refusing to extract ${tar_file}; unsafe archive path: ${unsafe_entry}"
        return 1
    fi
}

run_vximporter() {
    local import_file="$1"
    local vximporter_image="${VXIMPORTER_IMAGE:-ghcr.io/noaa-gsl/vximporter:latest}"
    local vximporter_workers="${VXIMPORTER_WORKERS:-16}"
    local vximporter_batch_size="${VXIMPORTER_BATCH_SIZE:-1000}"
    local container_import_file
    container_import_file="/opt/data/${hostname}/${pid}/temp_xfer/$(basename "${import_file}")"

    echo "Running vximporter container: ${vximporter_image}"
    echo "Importing ${import_file}; log: ${import_log_file}"
    docker run --rm \
    --mount "type=bind,source=${CREDENTIALS_FILE},target=/run/config/credentials,readonly" \
    --mount "type=bind,source=${import_file},target=${container_import_file},readonly" \
    "${vximporter_image}" \
    -conn "/run/config/credentials" \
    -file "${container_import_file}" \
    -workers "${vximporter_workers}" \
    -batch-size "${vximporter_batch_size}" 2>&1 | tee -a "${import_log_file}"
}

# Assign the job_id argument
job_id="$1"

# Validate that job_id starts with 'JS:'
if [[ ! "$job_id" =~ ^JS:.* ]]; then
    echo "Error: job_id must start with 'JS:'"
    exit 1
fi

echo "Submitting job with ID: $job_id"
working_root_dir="${WORKING_ROOT_DIR:-/data-ingest/data/working}"
public_dir="${PUBLIC_DIR:-/public}"
pid=$$
hostname=$(hostname)
temp_out_dir="${working_root_dir}/${hostname}/${pid}/temp_outdir"
temp_xfer_dir="${working_root_dir}/${hostname}/${pid}/temp_xfer"

log_dir="${working_root_dir}/logs"
metrics_dir="${working_root_dir}/common/job_metrics"

# Docker mounts host ${working_root_dir} to /opt/data in the container.
container_out_parent="/opt/data/${hostname}/${pid}/temp_outdir"
container_xfer_parent="/opt/data/${hostname}/${pid}/temp_xfer"
container_log_dir="/opt/data/logs"
container_metrics_dir="/opt/data/common/job_metrics"

# Ensure parent directories exist before mktemp and logging.
mkdir -p "${temp_out_dir}" "${temp_xfer_dir}" "${log_dir}" "${metrics_dir}"

# Create temporary directories for output and transfer
tmp_outdir=$(mktemp -d -p "${temp_out_dir}")
tmp_xfer=$(mktemp -d -p "${temp_xfer_dir}")
container_tmp_outdir="${container_out_parent}/$(basename "${tmp_outdir}")"
container_tmp_xfer="${container_xfer_parent}/$(basename "${tmp_xfer}")"

timestamp=$(date +%s)
ingest_log_file="${log_dir}/docker-ingest-${job_id}-${timestamp}.out"
import_log_file="${log_dir}/docker-import-${job_id}-${timestamp}.out"

# Optional: mount raw data directory if DATA_SOURCE is set
# DATA_SOURCE should point to the host directory containing input files
# CONTAINER_DATA_PATH specifies where to mount it in the container (default: same as host path)
vxingest_image="${VXINGEST_IMAGE:-ghcr.io/noaa-gsl/vxingest/ingest:latest}"
ingest_args=(
    docker run --rm
    --pull=always \
    --mount "type=bind,source=${working_root_dir},target=/opt/data"
    --mount "type=bind,source=${public_dir},target=/public,readonly"
    --mount "type=bind,source=${CREDENTIALS_FILE},target=/run/secrets/CREDENTIALS_FILE,readonly"
)
if [ -n "${DATA_SOURCE:-}" ]; then
    container_data_path="${CONTAINER_DATA_PATH:-${DATA_SOURCE}}"
    ingest_args+=(--mount "type=bind,source=${DATA_SOURCE},target=${container_data_path},readonly")
fi
if [ -n "${LOG_LEVEL:-}" ]; then
    ingest_args+=(--env "VXINGEST_LOG_LEVEL=${LOG_LEVEL}")
fi

# Run the ingest job using Docker
echo "Running VxIngest container: ${vxingest_image}; log: ${ingest_log_file}"
"${ingest_args[@]}" "${vxingest_image}" \
-c /run/secrets/CREDENTIALS_FILE \
-o "${container_tmp_outdir}" \
-l "${container_log_dir}" \
-m "${container_metrics_dir}" \
-x "${container_tmp_xfer}" \
-j "${job_id}" >"${ingest_log_file}" 2>&1

# Import job documents for the given job ID using vximporter.
# Imports every JSON or gzip-compressed JSON file found in the transfer output.
echo "importing job documents for job ID: $job_id"

# Debug: show what's in the directories
echo "Debug: Contents of tmp_outdir (${tmp_outdir}):"
ls -lR "${tmp_outdir}" || echo "  (directory empty or not accessible)"
echo "Debug: Contents of tmp_xfer (${tmp_xfer}):"
ls -lR "${tmp_xfer}" || echo "  (directory empty or not accessible)"

# First, look for all tar.gz files and extract them
found_tar_file=false
while IFS= read -r -d '' tar_file; do
    found_tar_file=true
    echo "Extracting: ${tar_file}"
    validate_tar_paths "${tar_file}"
    tar -xzf "${tar_file}" -C "${tmp_xfer}"
done < <(find "${tmp_xfer}" -maxdepth 1 -type f -name '*.tar.gz' -print0)
if [ "${found_tar_file}" = "true" ]; then
    echo "Finished extracting tar archives."
fi

# Now import all JSON files (either direct .json or .json.gz)
found_import_file=false
while IFS= read -r -d '' import_file; do
    found_import_file=true
    run_vximporter "${import_file}"
done < <(find "${tmp_xfer}" -type f \( -name '*.json' -o -name '*.json.gz' \) ! -path "*/.*" -print0)

if [ "${found_import_file}" != "true" ]; then
    echo "No JSON input found in ${tmp_xfer}; skipping import step."
    echo "Debug: Checking if any files exist in output or transfer directories..."
    find "${tmp_outdir}" -type f 2>/dev/null | head -20 || true
    find "${tmp_xfer}" -type f 2>/dev/null | head -20 || true
fi
