#!/usr/bin/env bash
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <job_id>"
    exit 1
fi

job_id="$1"

if [[ ! "$job_id" =~ ^JS:.* ]]; then
    echo "Error: job_id must start with 'JS:'"
    exit 1
fi

echo "Submitting job with ID: $job_id"

data=/data-ingest/data public=/public docker compose run --rm ingest -c /run/secrets/CREDENTIALS_FILE -o /opt/data/outdir -l /opt/data/logs -m /opt/data/common/job_metrics -x /opt/data/xfer -j ${job_id}

echo "importing job documents for job ID: $job_id"
cd /home/amb-verif/VxIngest && /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-import.sh -c /home/amb-verif/credentials -l /data-ingest/data/xfer/ -m /data-ingest/data/common/job_metrics -t /data-ingest/data/temp_tar  > /home/amb-verif/logs/cron-import-`date +\%s`.out 2>&1 && rm -rf $(find ~/logs -type f -name cron-import-*.out -size -100c -exec grep -l "No such file" {} \;)

