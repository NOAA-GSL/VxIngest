#!/usr/bin/env bash

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 MODEL_1 [MODEL2 ...]"
    exit 1
fi
if [[ "$(basename "$PWD")" != "VxIngest" ]]; then
    echo "Error: This script must be run from the VxIngest directory."
    exit 1
fi
for model_name in "$@"; do
    model_js="JS:METAR:MODEL:${model_name}:schedule:job:V01"
    echo "Processing model: $model_name with job ID: $model_js"
    ./scripts/VXingest_utilities/run_job.sh "$model_js"
    echo "Finished processing model: $model_name"
    echo "Processing CTC documents for model: $model_name"
    # CTC's are like JS:METAR:CTC:RRFSv2_conus_3km_ret_test4_may2024:schedule:job:V01
    ctc_id="JS:METAR:CTC:${model_name}:schedule:job:V01"
    ./scripts/VXingest_utilities/run_job.sh "$ctc_id"
    echo "Finished processing CTC documents for model: $model_name"
    # SUMS are like JS:METAR:SUMS:RRFSv2_conus_3km_ret_test4_may2024:schedule:job:V01
    sums_id="JS:METAR:SUMS:${model_name}:schedule:job:V01"
    ./scripts/VXingest_utilities/run_job.sh "$sums_id"
    echo "Finished processing SUMS documents for model: $model_name"
done
echo "update the metadata"
cd /home/amb-verif/VxIngest/meta_update_middleware && go run .
