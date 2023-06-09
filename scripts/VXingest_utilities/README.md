# How the scripts in this directory work with relationship to the cron jobs

## The cron jobs are run as user amb-verif

## The scripts in this directory are run as user amb-verif

Some of them (listed below) are run by the cron jobs.
Some of them are basic utilities that are used by tests or on the command line by amb-verif.
The scripts that are used by the cron jobs are listed below.

## Example of a vxIngest cron job

``` bash

[amb-verif@adb-cb1 VXingest_utilities]$ crontab -l
# run the ingest on 15 minute interval
*/15 * * * * /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-ingest.sh -c /home/amb-verif/adb-cb1-credentials -d /home/amb-verif/VxIngest -l /home/amb-verif/VxIngest/logs -o /data -m /data/common/job_metrics -x /data/temp > /home/amb-verif/logs/cron-ingest-`date +\%s`.out 2>&1

# run the import on two minute interval
*/2 * * * * /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-import.sh -c /home/amb-verif/adb-cb1-credentials -d /home/amb-verif/VxIngest -l /data/temp -m /data/common/job_metrics -t /data/temp_tar > /home/amb-verif/logs/cron-import-`date +\%s`.out 2>&1

#tar the log files on daily interval
10 * * * * tar czf "/home/amb-verif/logs/archive/cron-`date +\%s`.tar.gz" $(find /home/amb-verif/logs -type f -name "cron*.out" -cmin +1440) -C /home/amb-verif/logs --remove-files > /dev/null 2>&1
[amb-verif@adb-cb1 VXingest_utilities]$

```

### run-ingest.sh

This script is run by the cron job every 15 minutes.
It does not use any other scripts from this directory.
It queries the database for JOB documents and checks the status, the schedule, and the run_priority of each job.
The script will run the jobs in the order of run_priority, as the schedule for each JOB comes due.
Each JOB will be run in its own process. Each JOB will be run in its own python virtual environment.
Each JOB has a list of ingest_document_ids and each ingest_document_id specifies a set of documents to be ingested.
The script will run the ingest for each ingest_document_id in the JOB.

### run-import.sh

This script is run by the cron job every 2 minutes.
It uses the following scripts from this directory.

- import_docs.sh which uses the couchbase cbimport utility to import documents into the database.
- scrape_metrics.sh which uses the prometheus promql utility for getting historical data from prometheus.
- update metadata scripts to update the mats metadata documents in the database.
