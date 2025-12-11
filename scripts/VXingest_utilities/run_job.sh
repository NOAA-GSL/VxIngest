#!/usr/bin/env bash

# ------------------------------------------------------------------------------
# Script: run_job.sh
# Purpose: Submits and processes one or more VxIngest jobs by job ID.
#          For each job ID, this script:
#            - Validates the job ID format
#            - Submits the job using Docker Compose
#            - Runs an import script to process job documents
#            - Cleans up small/no-op log files
#          After all jobs, updates metadata using a Go program.
# Usage:
#   ./run_job.sh <job_id1> [<job_id2> ...]
#   Each job_id must start with 'JS:'.
# ------------------------------------------------------------------------------

# Exit on error, unset variable, or failed pipe
set -euo pipefail

# Check for at least one argument (job ID list)
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <job_id1 job_id2 ...>"
    exit 1
fi

# Collect job IDs from arguments
job_ids=( "$@" )

for job_id in "${job_ids[@]}"; do
    echo "Processing job_id: $job_id"
    # Validate job_id format (must start with 'JS:')
    if [[ ! "$job_id" =~ ^JS:.* ]]; then
        echo "Error: job_id must start with 'JS:' $job_id"
        exit 1
    fi

    echo "Submitting job with ID: $job_id"
    # Submits the job using Docker Compose. Environment variables 'data' and 'public' are set for the container.
    docker compose run --rm \
    -e data=/data-ingest/data \
    -e public=/public \
    ingest \
    -c /run/secrets/CREDENTIALS_FILE \
    -o /opt/data/outdir \
    -l /opt/data/logs \
    -m /opt/data/common/job_metrics \
    -x /opt/data/xfer \
    -j "$job_id"

    echo "Importing job documents for job ID: $job_id"
    # Runs the import script to process job documents for this job ID.
    cd /home/amb-verif/VxIngest
    /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-import.sh \
    -c /home/amb-verif/credentials \
    -l /data-ingest/data/xfer/ \
    -m /data-ingest/data/common/job_metrics \
    -t /data-ingest/data/temp_tar \
    > "/home/amb-verif/logs/cron-import-$(date +%s).out" 2>&1

    # Clean up small/no-op log files (less than 100 bytes and containing 'No such file')
    find ~/logs -type f -name 'cron-import-*.out' -size -100c -exec grep -l "No such file" {} \; -delete
done

echo "add this retro to the metadata"


echo "Updating the metadata"
# After all jobs, update the metadata using the Go program in meta_update_middleware.
cd /home/amb-verif/VxIngest/meta_update_middleware
go run .
