#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage: clone_model.sh -m MODEL_NAME -n NEW_MODEL -e EXISTING_PATH -f FILE_PATH -c CREDENTIALS

Options:
	-m, --modelname MODEL_NAME       Existing model name.
	-n, --newmodel NEW_MODEL         New model name.
	-e, --existingpath EXISTING_PATH Existing path to replace.
	-f, --filepath FILE_PATH         New file path.
	-c, --credentials CREDENTIALS    Couchbase credentials file.
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
            args|credentials|escaped_existing_file_path|escaped_file_path|escaped_model_name|escaped_new_model|existing_file_path|file_path|model_name|new_model|param)
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

escape_sed_pattern() {
    local value="$1"

    value="${value//\\/\\\\}"
    value="${value//|/\\|}"
    value="${value//./\\.}"
    value="${value//[/\\[}"
    value="${value//]/\\]}"
    value="${value//\*/\\*}"
    value="${value//^/\\^}"
    value="${value//\$/\\$}"
    printf '%s' "${value}"
}

escape_sed_replacement() {
    local value="$1"

    value="${value//\\/\\\\}"
    value="${value//&/\\&}"
    value="${value//|/\\|}"
    printf '%s' "${value}"
}

if [[ $# -eq 0 ]]; then
    usage >&2
    exit 1
fi

args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --modelname)
            if [[ $# -lt 2 || "$2" == -* ]]; then
                echo "Error: --modelname requires an argument." >&2
                usage >&2
                exit 1
            fi
            args+=(-m "$2")
            shift 2
        ;;
        --modelname=*)
            args+=(-m "${1#*=}")
            shift
        ;;
        --filepath)
            if [[ $# -lt 2 || "$2" == -* ]]; then
                echo "Error: --filepath requires an argument." >&2
                usage >&2
                exit 1
            fi
            args+=(-f "$2")
            shift 2
        ;;
        --filepath=*)
            args+=(-f "${1#*=}")
            shift
        ;;
        --newmodel)
            if [[ $# -lt 2 || "$2" == -* ]]; then
                echo "Error: --newmodel requires an argument." >&2
                usage >&2
                exit 1
            fi
            args+=(-n "$2")
            shift 2
        ;;
        --newmodel=*)
            args+=(-n "${1#*=}")
            shift
        ;;
        --existingpath)
            if [[ $# -lt 2 || "$2" == -* ]]; then
                echo "Error: --existingpath requires an argument." >&2
                usage >&2
                exit 1
            fi
            args+=(-e "$2")
            shift 2
        ;;
        --existingpath=*)
            args+=(-e "${1#*=}")
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

model_name=""
new_model=""
existing_file_path=""
file_path=""
credentials=""

while getopts ":m:n:e:f:c:h" param; do
    case "${param}" in
        m)
            model_name="${OPTARG}"
        ;;
        n)
            new_model="${OPTARG}"
        ;;
        e)
            existing_file_path="${OPTARG}"
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

if [[ -z "${model_name}" ]]; then
    echo "Error: model name is required." >&2
    usage >&2
    exit 1
fi

if [[ -z "${file_path}" ]]; then
    echo "Error: file path is required." >&2
    usage >&2
    exit 1
fi

if [[ -z "${new_model}" ]]; then
    echo "Error: new model is required." >&2
    usage >&2
    exit 1
fi

if [[ -z "${existing_file_path}" ]]; then
    echo "Error: existing path is required." >&2
    usage >&2
    exit 1
fi

if [[ -z "${credentials}" ]]; then
    echo "Error: credentials file is required." >&2
    usage >&2
    exit 1
fi

if [[ ! -e "${existing_file_path}" ]]; then
    echo "Error: existing path does not exist: ${existing_file_path}" >&2
    exit 1
fi

if [[ ! -e "${file_path}" ]]; then
    echo "Error: file path does not exist: ${file_path}" >&2
    exit 1
fi

if [[ ! -f "${credentials}" ]]; then
    echo "Error: credentials file does not exist or is not a regular file: ${credentials}" >&2
    exit 1
fi

require_command yq "to parse credentials file: ${credentials}"
require_command cbq "to query Couchbase"
require_command jq "to parse cbq output"

cb_host=""
cb_user=""
cb_password=""
load_credentials "${credentials}"

if [[ -z "${cb_host}" || -z "${cb_user}" || -z "${cb_password}" ]]; then
    echo "Error: credentials file must define cb_host, cb_user, and cb_password." >&2
    exit 1
fi

escaped_model_name="$(escape_sed_pattern "${model_name}")"
escaped_existing_file_path="$(escape_sed_pattern "${existing_file_path}")"
escaped_new_model="$(escape_sed_replacement "${new_model}")"
escaped_file_path="$(escape_sed_replacement "${file_path}")"
cbq -no-ssl-verify -q -e "${cb_host}" -u "${cb_user}" -p "${cb_password}" -s "SELECT RUNTIME.* FROM vxdata._default.RUNTIME WHERE (type='JS' OR type='PS' OR type='DS' OR type='IS') AND CONTAINS(meta().id,'${model_name}');" | grep -v Disabling | sed "s|${escaped_model_name}|${escaped_new_model}|g" | sed "s|${escaped_existing_file_path}|${escaped_file_path}|g" | jq .results
