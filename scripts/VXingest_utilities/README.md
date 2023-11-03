# How the ingest processes are run

## Overview

The ingest processing is divided into two parts.
1 The run-ingest.sh script
2 The run-import.sh script

### cron table

For processing on adb-cb1 we use the following cron entry

``` bash
# run the ingest on 15 minute interval
*/15 * * * * /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-ingest.sh -c /home/amb-verif/adb-cb1-credentials -d /home/amb-verif/VxIngest -l /home/amb-verif/VxIngest/logs -o /data -m /data/common/job_metrics -x /data/temp > /home/amb-verif/logs/cron-ingest-`date +\%s`.out 2>&1

# run the import on two minute interval
*/2 * * * * /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-import.sh -c /home/amb-verif/adb-cb1-credentials -d /home/amb-verif/VxIngest -l /data/temp -m /data/common/job_metrics -t /data/temp_tar > /home/amb-verif/logs/cron-import-`date +\%s`.out 2>&1

#tar the log files on daily interval
10 22 * * * tar czf "/home/amb-verif/logs/archive/cron-`date +\%s`.tar.gz" $(find /home/amb-verif/logs -type f -name "cron*.out" -cmin +1440) -C /home/amb-verif/logs --remove-files > /dev/null 2>&1

# backup the cluster daily at midnight
0 0 * * *  /couchbase/backup/cluster/nightlybackup.sh > /dev/null 2>&1

# backup the standalone daily at 1:00 AM
0 1 * * *  /couchbase/backup/standalone/nightlybackup.sh > /dev/null 2>&1
```

The last three entries are self explanatory. The first entry actually runs the ingest processes and the second entry imports the data and runs the metrics scraper.

These two process steps are designed to be run in different processing contexts. For example the run-ingest.sh could potentially be run on HPC while the run-import.sh is designed to be run on our standalone internal couchbase server.

### run-ingest.sh

This script performs the fllowing steps...
1 