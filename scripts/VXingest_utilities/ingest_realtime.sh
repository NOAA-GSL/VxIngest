#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Script: ingest_realtime.sh
# Description: Runs ingest jobs for a model, and associated CTC and SUMS
#              documents, and updates metadata. Must be run from the
#              VxIngest directory. The script takes a job spec i.e.
# Usage: ./ingest_retro.sh MODEL_1 [MODEL2 ...]
# ------------------------------------------------------------------------------

cbq_conn() {
    usage() {
        echo "$0 -e|--endpoint endpoint ca_root_cert_file -u|--user|--username username -p|--pass|--password password"
        exit 1
    }
    local endpoint=""
    local username=""
    local password=''
    local extra_args=()

    # Parse named options
    while [[ $# -gt 0 ]]; do
        case "$1" in
        -e | --endpoint)
            endpoint="$2"
            shift 2
            ;;
        -u | --user | --username)
            username="$2"
            shift 2
            ;;
        -p | --pass | --password)
            password="$2"
            shift 2
            ;;
        *)
            extra_args+=("$1")
            shift
            ;;
        esac
    done

    if [[ -z $endpoint ]] || [[ $username ]] || [[ $password ]]; then
        usage
    fi

    # Execute cbq with resolved values
    cbq -no-ssl-verify \
        -q \
        -e "$endpoint" \
        -u "$username" \
        -p "$password" \
        "${extra_args[@]}"
}

# main
# Check that the CREDENTIALS_FILE environment variable is set
if [ -z "$CREDENTIALS" ]; then
    echo "Error: CREDENTIALS environment variable is not set."
    exit 1
fi  

# Ensure one model job spec is provided as an argument and extract the model value
if [[ $# -lt 1 ]] || [[ $# -gt 1 ]]; then
    echo "Usage: $0 MODEL_JOB_SPEC"
    echo "example: $0 'JS:METAR:MODEL:HRRR_OPS:schedule:job:V01'"
    exit 1
fi
if [[ ! "$1" =~ '^JS:METAR:MODEL:(.*):schedule:job:V01$' ]]; then
    echo "Error: Invalid parameter format."
    exit 1
fi

model_js=$1
model_name="${BASH_REMATCH[1]}"

# Check that the script is run from the VxIngest directory
if [[ "$(basename "$PWD")" != "VxIngest" ]]; then
    echo "Error: This script must be run from the VxIngest directory."
    exit 1
fi

# Model data must be in the database prior to running CTCs or SUMS
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

# Update the metadata after all jobs are processed
echo "update the metadata"
cd /home/amb-verif/VxIngest/meta_update_middleware && go run .
