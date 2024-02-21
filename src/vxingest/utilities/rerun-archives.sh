ls -1tr /data-ingest/data/xfer/rerun | while read f; do echo mv $f /data-ingest/data/xfer; count=$(($count+1)); if [[ $(($count%30)) -eq 0 ]]; then sleep 300;fi; done
