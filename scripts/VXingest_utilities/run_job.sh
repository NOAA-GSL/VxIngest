
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


# Create temporary directories for tar, output, and transfer
echo "Submitting job with ID: $job_id"
tmp_tardir=$(mktemp -d, -p "/opt/data/tmp_tar")
tmp_outdir=$(mktemp -d, -p "/opt/data/outdir")
tmp_xfer=$(mktemp -d, -p "/opt/data/xfer")
log_dir="/opt/data/logs"
metrics_dir="/opt/data/common/job_metrics"

# Run the ingest job using Docker Compose
data=/opt/data public=/public docker compose run --rm ingest -c /run/secrets/CREDENTIALS_FILE -o ${tmp_outdir} -l ${log_dir} -m ${metrics_dir} -x ${tmp_xfer} -j ${job_id}


# Import job documents for the given job ID
echo "importing job documents for job ID: $job_id"
cd /home/amb-verif/VxIngest && \
/home/amb-verif/VxIngest/scripts/VXingest_utilities/run-import.sh \
-c /home/amb-verif/credentials \
-l ${tmp_xfer} \
-m ${metrics_dir} \
-t ${tmp_tardir} \
> ${HOME}/logs/import-`date +%s`.out 2>&1 && \
rm -rf $(find ${HOME}/logs -type f -name import-*.out -size -100c -exec grep -l "No such file" {} \;)
