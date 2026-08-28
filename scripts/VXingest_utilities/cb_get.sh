#!/usr/bin/env bash
    
set -euo pipefail 

# Helper function for usage info
show_help() {
    echo "Usage: cb-get <doc_id> [options]"
    echo "Retrieves a Couchbase document using cbc-cat."
    echo ""
    echo "Arguments:"
    echo "  doc_id                  The document ID to fetch (Required)"
    echo ""
    echo "Options:"
    echo "  -b, --bucket <name>     The bucket name (default: 'vxdata')"
    echo "  -s, --scope <name>      The scope name (default: '_default')"
    echo "  -c, --collection <name> The collection name (default: 'METAR')"
    echo "  -h, --help              Display this help message"
}

# Show error and help if NO arguments are provided at all
if [[ "$#" -eq 0 ]]; then
    echo "Error: Document ID is required." >&2
    echo "" >&2
    show_help
    exit 1
fi

# Show help if explicitly requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi

# Check if the first argument incorrectly starts with a flag
if [[ "$1" == -* ]]; then
    echo "Error: Document ID is required as the first argument." >&2
    echo "Try 'cb-get --help' for more information." >&2
    exit 1
fi

doc_id="$1"
shift # Move past the document ID

# Set your custom default values
bucket="vxdata"
scope="_default"
collection="METAR"

# Parse options/flags
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -b|--bucket)
            bucket="$2"
            shift 2
            ;;
        -s|--scope)
            scope="$2"
            shift 2
            ;;
        -c|--collection)
            collection="$2"
            shift 2
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            echo "Try 'cb-get --help' for usage details." >&2
            exit 1
            ;;
    esac
done

# Load credentials
eval "$(yq '. | to_entries | map("export " + .key + "=" + (.value | tostring | @sh)) | .[]' ~/credentials-capella)"

# Execute cbc cat
cbc cat "${doc_id}" \
    -u "${cb_user}" \
    -P "${cb_password}" \
    -U "${cb_host}/${bucket}" \
    --scope="${scope}" \
    --collection="${collection}"        
