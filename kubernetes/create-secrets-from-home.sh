#!/usr/bin/env bash
set -euo pipefail

# Creates or updates runtime secrets from local files in HOME.
KUBECONFIG_PATH="${KUBECONFIG_PATH:-${HOME}/.kube/development.yaml}"
NAMESPACE="${NAMESPACE:-vxingest-dev}"
SECRET_NAME="${SECRET_NAME:-vxingest-credentials}"
CACERT_SECRET_NAME="${CACERT_SECRET_NAME:-vxingest-cacert}"
CREDENTIALS_FILE="${CREDENTIALS_FILE:-${HOME}/credentials.yaml}"
CACERT_FILE="${CACERT_FILE:-${HOME}/capella-root-certificate.pem}"
DRY_RUN="${DRY_RUN:-false}"

CB_CA_FILE="${CB_CA_FILE:-${HOME}/.ssh/cb-ca.pem}"
require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "Missing required file: $path" >&2
        exit 1
    fi
}

# There also needs to be an imagePullSecret created independently for GHCR access,
# but that is not handled by this script since it is a one-time setup per cluster.
# It needs to be created like this...
# kubectl --kubeconfig=${HOME}/.kube/development.yaml \
# -n vxingest-dev create secret docker-registry ghcr  \
# --docker-server=ghcr.io  --docker-username=<a user name> \
# --docker-password=<associated PAT with read:packages permission and granted SSO access to the GSL GitHub org>

if ! command -v kubectl >/dev/null 2>&1; then
    echo "kubectl is required but was not found in PATH" >&2
    exit 1
fi

require_file "$KUBECONFIG_PATH"
require_file "$CREDENTIALS_FILE"

if [[ "$DRY_RUN" == "true" ]]; then
    if ! kubectl --kubeconfig="$KUBECONFIG_PATH" get namespace "$NAMESPACE" >/dev/null 2>&1; then
        echo "DRY_RUN=true: namespace $NAMESPACE does not exist or is not accessible" >&2
        exit 1
    fi
else
    if ! kubectl --kubeconfig="$KUBECONFIG_PATH" get namespace "$NAMESPACE" >/dev/null 2>&1; then
        echo "Namespace $NAMESPACE does not exist; creating it"
        kubectl --kubeconfig="$KUBECONFIG_PATH" create namespace "$NAMESPACE"
    fi
fi

secret_args=(
    --from-file=credentials.yaml="$CREDENTIALS_FILE"
)

if [[ -f "$CB_CA_FILE" ]]; then
    secret_args+=(--from-file=cb-ca.pem="$CB_CA_FILE")
    echo "Including Couchbase CA bundle from $CB_CA_FILE"
else
    echo "No Couchbase CA bundle found at $CB_CA_FILE; cb-ca.pem will not be included" >&2
fi

secret_manifest_cmd=(
    kubectl
    --kubeconfig="$KUBECONFIG_PATH"
    --namespace
    "$NAMESPACE"
    create
    secret
    generic
    "$SECRET_NAME"
    "${secret_args[@]}"
    --dry-run=client
    -o
    yaml
)

if [[ "$DRY_RUN" == "true" ]]; then
    "${secret_manifest_cmd[@]}"
    if [[ -f "$CACERT_FILE" ]]; then
        kubectl --kubeconfig="$KUBECONFIG_PATH" --namespace "$NAMESPACE" create secret generic "$CACERT_SECRET_NAME" \
        --from-file=cacert.pem="$CACERT_FILE" \
        --dry-run=client -o yaml
    else
        echo "No CA certificate file found at $CACERT_FILE; skipped rendering $CACERT_SECRET_NAME" >&2
    fi
    echo "DRY_RUN=true: rendered secret manifest for $SECRET_NAME; no changes were applied."
else
    "${secret_manifest_cmd[@]}" | kubectl --kubeconfig="$KUBECONFIG_PATH" --namespace "$NAMESPACE" apply -f -
    if [[ -f "$CACERT_FILE" ]]; then
        kubectl --kubeconfig="$KUBECONFIG_PATH" --namespace "$NAMESPACE" create secret generic "$CACERT_SECRET_NAME" \
        --from-file=cacert.pem="$CACERT_FILE" \
        --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_PATH" --namespace "$NAMESPACE" apply -f -
    else
        echo "No CA certificate file found at $CACERT_FILE; skipped applying $CACERT_SECRET_NAME" >&2
    fi
    echo "Secret $SECRET_NAME applied in namespace $NAMESPACE using home-directory files."
fi

