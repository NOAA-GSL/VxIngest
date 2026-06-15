# VXingest Utility Scripts

## Overview

Ingest processing is split into two primary phases:

1. `run-ingest.sh` builds output artifacts.
2. `run-import.sh` imports JSON artifacts into Couchbase.

`run-import.sh` loads JSON documents and optionally runs metadata updates.

## Typical Scheduling

Example cron entries:

```bash
# run ingest every 15 minutes
*/15 * * * * /home/amb-verif/VxIngest/scripts/VXingest_utilities/run-ingest.sh ...

# run import every 2 minutes
*/2 * * * * /home/amb-verif/VxIngest/scripts/VXingest_utilities/import/run-import.sh ...
```

The import scripts are designed for frequent execution and are safe to run on a cadence where multiple invocations can overlap.

## run-import.sh

See import directory
