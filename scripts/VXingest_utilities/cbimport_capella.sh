#!/usr/bin/env bash

set -euo pipefail

if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    echo "Error: This script must be executed, not sourced." >&2
    echo "Usage: ./cbimport_capella.sh [options]" >&2
    return 1
fi

usage() {
    cat <<'EOF'
Usage: cbimport_capella.sh -C COLLECTION -f FILE -c CREDENTIALS

Options:
    -C, --collection COLLECTION   Target Couchbase collection.
    -f, --file FILE               JSON file to import.
    -c, --credentials CREDENTIALS Couchbase credentials YAML file.
EOF
}

load_credentials() {
    local credentials_file="$1"
    local key
    local value

    while IFS=$'\t' read -r key value; do
        if [[ ! "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
            echo "Error: credentials key is not a valid shell variable name: ${key}" >&2
            exit 1
        fi
        case "${key}" in
            args|collection|credentials|file_path|key|param|value)
                echo "Error: credentials key conflicts with a script variable: ${key}" >&2
                exit 1
            ;;
        esac
        printf -v "${key}" '%s' "${value}"
    done < <(yq -r 'to_entries[] | [.key, (.value // "")] | @tsv' "${credentials_file}")
}

require_command() {
    local command_name="$1"
    local description="$2"

    if ! command -v "${command_name}" >/dev/null 2>&1; then
        echo "Error: ${command_name} is required ${description}, but it was not found in PATH." >&2
        exit 1
    fi
}

if [[ $# -eq 0 ]]; then
    usage >&2
    exit 1
fi

args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --collection)
            if [[ $# -lt 2 || "$2" == -* ]]; then
                echo "Error: --collection requires an argument." >&2
                usage >&2
                exit 1
            fi
            args+=(-C "$2")
            shift 2
        ;;
        --collection=*)
            args+=(-C "${1#*=}")
            shift
        ;;
        --file)
            if [[ $# -lt 2 || "$2" == -* ]]; then
                echo "Error: --file requires an argument." >&2
                usage >&2
                exit 1
            fi
            args+=(-f "$2")
            shift 2
        ;;
        --file=*)
            args+=(-f "${1#*=}")
            shift
        ;;
        --credentials)
            if [[ $# -lt 2 || "$2" == -* ]]; then
                echo "Error: --credentials requires an argument." >&2
                usage >&2
                exit 1
            fi
            args+=(-c "$2")
            shift 2
        ;;
        --credentials=*)
            args+=(-c "${1#*=}")
            shift
        ;;
        --help)
            usage
            exit 0
        ;;
        *)
            args+=("$1")
            shift
        ;;
    esac
done
if [[ ${#args[@]} -eq 0 ]]; then
    set --
else
    set -- "${args[@]}"
fi

collection=""
file_path=""
credentials=""

while getopts ":C:f:c:h" param; do
    case "${param}" in
        C)
            collection="${OPTARG}"
        ;;
        f)
            file_path="${OPTARG}"
        ;;
        c)
            credentials="${OPTARG}"
        ;;
        h)
            usage
            exit 0
        ;;
        :)
            echo "Error: -${OPTARG} requires an argument." >&2
            usage >&2
            exit 1
        ;;
        \?)
            echo "Error: invalid option -${OPTARG}." >&2
            usage >&2
            exit 1
        ;;
    esac
done

if [[ -z "${collection}" ]]; then
    echo "Error: collection is required." >&2
    usage >&2
    exit 1
fi

if [[ -z "${file_path}" ]]; then
    echo "Error: file is required." >&2
    usage >&2
    exit 1
fi

if [[ -z "${credentials}" ]]; then
    echo "Error: credentials file is required." >&2
    usage >&2
    exit 1
fi

if [[ ! -f "${file_path}" ]]; then
    echo "Error: import file does not exist or is not a regular file: ${file_path}" >&2
    exit 1
fi

if [[ ! -f "${credentials}" ]]; then
    echo "Error: credentials file does not exist or is not a regular file: ${credentials}" >&2
    exit 1
fi

require_command yq "to parse credentials file: ${credentials}"
require_command cbimport "to import JSON into Couchbase"

cb_host=""
cb_user=""
cb_password=""
cb_bucket="vxdata"
cb_scope="_default"
cb_ca_cert=""
load_credentials "${credentials}"

if [[ -z "${cb_host}" || -z "${cb_user}" || -z "${cb_password}" ]]; then
    echo "Error: credentials file must define cb_host, cb_user, and cb_password." >&2
    exit 1
fi

cbimport_args=(
    cbimport json --no-ssl-verify
    --format list
    --cluster "${cb_host}"
    --username "${cb_user}"
    --password "${cb_password}"
    --bucket "${cb_bucket}"
    --scope-collection-exp "${cb_scope}.${collection}"
    --dataset "file://${file_path}"
    --generate-key '%id%'
)

if [[ -n "${cb_ca_cert}" ]]; then
    cbimport_args+=(--cacert "${cb_ca_cert}")
fi

"${cbimport_args[@]}"