#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Script: run_job.sh
# Description: Submits and processes a VxIngest job using Docker Compose and
#              handles job document import. Requires CREDENTIALS_FILE env var.
# Usage: ./run_job.sh <job_id>
# ------------------------------------------------------------------------------

# Check that the CREDENTIALS_FILE environment variable is set
if [ -z "$CREDENTIALS_FILE" ]; then
    echo "Error: CREDENTIALS_FILE environment variable is not set."
    exit 1
fi

# Ensure exactly one argument (job_id) is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <job_id>"
    exit 1
fi

# Assign the job_id argument
job_id="$1"

# Validate that job_id starts with 'JS:'
if [[ ! "$job_id" =~ ^JS:.* ]]; then
    echo "Error: job_id must start with 'JS:'"
    exit 1
fi

echo "Submitting job with ID: $job_id"
export working_root_dir="/data-ingest/data/working"
export archive_dir="/data/ingest/data/archive"
pid=$$
hostname=$(hostname)
temp_out_dir="${working_root_dir}/${hostname}/${pid}/temp_outdir"
temp_xfer_dir="${working_root_dir}/${hostname}/${pid}/temp_xfer"
temp_tar_dir="${working_root_dir}/${hostname}/${pid}/temp_tar"

log_dir="${working_root_dir}/logs"
metrics_dir="${working_root_dir}/common/job_metrics"

# Create temporary directories for tar, output, and transfer
tmp_outdir=$(mktemp -d -p "${temp_out_dir}")
tmp_xfer=$(mktemp -d -p "${tem_xfer_dir}")
tmp_tar=$(mktemp -d -p "${tem_tar_dir}")

# Run the ingest job using Docker Compose
data=${working_root_dir} public=/public docker compose run --rm ingest -c /run/secrets/CREDENTIALS_FILE -o ${tmp_outdir} -l ${log_dir} -m ${metrics_dir} -x ${tmp_xfer} -j ${job_id} >${log_dir}/docker-ingest-${job_id}-$(date +\%s).out 2>&1

# Import job documents for the given job ID
echo "importing job documents for job ID: $job_id"
cd /home/amb-verif/VxIngest &&
    docker compose run --rm import \
        -a / ${archive_dir} \
        -c /runsecrets/CREDENTIALS_FILE \
        -l ${tmp_xfer} \
        -t ${tmp_tar} >${log_dir}/docker-import-${job_id}-$(date +\%s).out 2>&1
