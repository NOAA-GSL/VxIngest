#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Script: ingest_model.sh
# Description: Runs ingest jobs for one or more models, including CTC and SUMS
#              document processing, and updates metadata.
# Usage: ./ingest_model.sh MODEL_1_JS [MODEL2_JS ...]
# ------------------------------------------------------------------------------

set -uo pipefail

run_job_or_exit() {
    local job_id="$1"
    if ! ./scripts/VXingest_utilities/run_job.sh "${job_id}"; then
        echo "Error: run_job.sh failed for job ID: ${job_id}" >&2
        exit 1
    fi
}

# Ensure at least one model job_spec is provided as an argument
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 MODEL_1_JS [MODEL2_JS ...]"
    exit 1
fi

if [ -z "${CREDENTIALS_FILE:-}" ]; then
    echo "Error: CREDENTIALS_FILE environment variable is not set."
    exit 1
fi

working_root_dir="${WORKING_ROOT_DIR:-/data-ingest/data/working}"
metadata_updater_image="${VX_METADATA_UPDATER_IMAGE:-ghcr.io/noaa-gsl/vxmetadataupdater:latest}"
metadata_updater_settings="${VX_METADATA_UPDATER_SETTINGS:-}"
metadata_updater_settings_container="/app/settings.json"

if [[ -z "${metadata_updater_settings}" ]]; then
    echo "Error: VX_METADATA_UPDATER_SETTINGS environment variable is not set." >&2
    echo "Set it to the host path of the VxMetadataUpdater settings.json file." >&2
    exit 1
fi

if [[ ! -r "${metadata_updater_settings}" ]]; then
    echo "Error: VX_METADATA_UPDATER_SETTINGS does not point to a readable file: ${metadata_updater_settings}" >&2
    exit 1
fi

# Loop through each provided model name and process jobs sequentially
for model_js in "$@"; do
    # Construct and process the MODEL job ID
    # Model data must be in the database prior to running CTCs or SUMS
    # run_job.sh creates the data and imports it into the database
    echo "Processing model with job ID: $model_js"
    run_job_or_exit "$model_js"
    echo "Finished processing model with job ID: $model_js"
    model_name="$(echo "$model_js" | cut -d: -f4)"
    
    # Construct and process the CTC job ID
    echo "Processing CTC documents for model: $model_name"
    # CTC ids are like JS:METAR:CTC:RRFSv2_conus_3km_ret_test4_may2024:schedule:job:V01
    ctc_id="JS:METAR:CTC:${model_name}:schedule:job:V01"
    run_job_or_exit "$ctc_id"
    echo "Finished processing CTC documents for model: $model_name"
    
    # Construct and process the SUMS job ID
    echo "Processing SUMS documents for model: $model_name"
    # SUMS ids are like JS:METAR:SUMS:RRFSv2_conus_3km_ret_test4_may2024:schedule:job:V01
    sums_id="JS:METAR:SUMS:${model_name}:schedule:job:V01"
    run_job_or_exit "$sums_id"
    echo "Finished processing SUMS documents for model: $model_name"
done

# Update the metadata after all jobs are processed
echo "update the metadata"
echo "Running VxMetadataUpdater container: ${metadata_updater_image}"
if ! docker run --rm \
--pull=always \
--env CREDENTIALS=/run/secrets/CREDENTIALS_FILE \
--mount "type=bind,source=${working_root_dir},target=/opt/data" \
--mount "type=bind,source=${CREDENTIALS_FILE},target=/run/secrets/CREDENTIALS_FILE,readonly" \
--mount "type=bind,source=${metadata_updater_settings},target=${metadata_updater_settings_container},readonly" \
"${metadata_updater_image}" \
-c /run/secrets/CREDENTIALS_FILE \
-s "${metadata_updater_settings_container}"; then
    echo "Error: VxMetadataUpdater failed" >&2
    exit 1
fi
echo "Success, VxMetadataUpdater - metadata updated"
