#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Script: ingest_retro.sh
# Description: Runs ingest jobs for one or more models, including CTC and SUMS
#              document processing, and updates metadata. Must be run from the
#              VxIngest directory.
# Usage: ./ingest_retro.sh MODEL_1 [MODEL2 ...]
# ------------------------------------------------------------------------------

# Ensure at least one model name is provided as an argument
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 MODEL_1 [MODEL2 ...]"
    exit 1
fi

# Check that the script is run from the VxIngest directory
if [[ "$(basename "$PWD")" != "VxIngest" ]]; then
    echo "Error: This script must be run from the VxIngest directory."
    exit 1
fi

# Loop through each provided model name and process jobs sequentially
for model_name in "$@"; do
    # Construct and process the MODEL job ID
    # Model data must be in the database prior to running CTCs or SUMS
    # run_job.sh creates the data and imports it into the database
    model_js="JS:METAR:MODEL:${model_name}:schedule:job:V01"
    echo "Processing model: $model_name with job ID: $model_js"
    ./scripts/VXingest_utilities/run_job.sh "$model_js"
    echo "Finished processing model: $model_name"

    # Construct and process the CTC job ID
    echo "Processing CTC documents for model: $model_name"
    # CTC ids are like JS:METAR:CTC:RRFSv2_conus_3km_ret_test4_may2024:schedule:job:V01
    ctc_id="JS:METAR:CTC:${model_name}:schedule:job:V01"
    ./scripts/VXingest_utilities/run_job.sh "$ctc_id"
    echo "Finished processing CTC documents for model: $model_name"

    # Construct and process the SUMS job ID
    echo "Processing SUMS documents for model: $model_name"
    # SUMS ids are like JS:METAR:SUMS:RRFSv2_conus_3km_ret_test4_may2024:schedule:job:V01
    sums_id="JS:METAR:SUMS:${model_name}:schedule:job:V01"
    ./scripts/VXingest_utilities/run_job.sh "$sums_id"
    echo "Finished processing SUMS documents for model: $model_name"
done

# Update the metadata after all jobs are processed
echo "update the metadata"
cd /home/amb-verif/VxIngest/meta_update_middleware && go run .
