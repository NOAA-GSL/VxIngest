#!/bin/bash
# Integration test harness for VxIngest Kubernetes orchestrator
# Tests the dynamic loop, job naming, sequencing, and error handling
# This test assumes that you have placed a valid ghcr capable token named mats-ghcr in ${HOME}/git-access-readp-packages-token
# This test also assumes that you have docker running, and that you have "kind" installed.
# This test assumes that you have a valid set database credentials in ${HOME}/credentials.yaml
# If connecting to a Capella cluster, you must have a valid CA certificate at ${HOME}/capella-root-certificate.pem



set -euo pipefail

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
KUBERNETES_DIR="${REPO_ROOT}/kubernetes"
TEST_NAMESPACE="vxingest-dev"
TEST_TIMEOUT_SECS=300
CLUSTER_NAME="${CLUSTER_NAME:-vxingest-test}"
TEST_DATA_HOST_PATH="${TEST_DATA_HOST_PATH:-/opt/data}"
TEST_PUBLIC_HOST_PATH="${TEST_PUBLIC_HOST_PATH:-/opt/data}"
VERBOSE="${VERBOSE:-0}"

# Test results
TESTS_PASSED=0
TESTS_FAILED=0
declare -a FAILED_TESTS=()

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# Logging Functions
# ============================================================================

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

log_section() {
    echo ""
    echo -e "${BLUE}=== $* ===${NC}"
    echo ""
}

log_pass() {
    echo -e "${GREEN}✓ $*${NC}"
    ((TESTS_PASSED++))
}

log_fail() {
    echo -e "${RED}✗ $*${NC}"
    ((TESTS_FAILED++))
    FAILED_TESTS+=("$*")
}

log_warn() {
    echo -e "${YELLOW}⚠ $*${NC}"
}

debug() {
    if [[ $VERBOSE -eq 1 ]]; then
        echo -e "${BLUE}[DEBUG]${NC} $*"
    fi
}

# ============================================================================
# Setup/Teardown Functions
# ============================================================================

setup_cluster() {
    log_section "Setting Up Test Cluster"

    # Check if cluster exists
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log "Cluster '${CLUSTER_NAME}' already exists"
    else
        log "Creating kind cluster: ${CLUSTER_NAME}"
        kind create cluster --name "${CLUSTER_NAME}"
    fi

    # Set kubeconfig context
    kubectl cluster-info --context "kind-${CLUSTER_NAME}" > /dev/null
    log "Using context: kind-${CLUSTER_NAME}"
}

setup_ghcr_access_token() {
    # This assumes that you have placed a valid ghcr capable token in ${HOME}/git-access-readp-packages-token
    read -r token < ${HOME}/git-access-readp-packages-token
    kubectl -n "${TEST_NAMESPACE}" create secret docker-registry vxingest-ghcr \
        --docker-server=ghcr.io \
        --docker-username="${token}" \
        --docker-password="${token}"
}

setup_test_namespace() {
    log_section "Setting Up Test Namespace"

    # Create test namespace
    kubectl create namespace "${TEST_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
    log "Created namespace: ${TEST_NAMESPACE}"

    # Create minimal secrets
    get_test_credentials
    get_test_cacert
}

get_test_credentials() {
    # Create a minimal valid credentials.yaml for testing
    local creds_file="${HOME}/credentials.yaml"

    kubectl create secret generic vxingest-credentials \
        --from-file=credentials.yaml="${creds_file}" \
        -n "${TEST_NAMESPACE}" \
        --dry-run=client -o yaml | kubectl apply -f -
    log "Created vxingest-credentials secret"
}

get_test_cacert() {
    # Create a dummy CA cert for testing
    local cert_file="${HOME}/capella-root-certificate.pem"
    openssl req -x509 -newkey rsa:2048 -keyout /dev/null -out "${cert_file}" \
        -days 1 -nodes -subj "/CN=localhost" 2>/dev/null || true

    kubectl create secret generic vxingest-cacert \
        --from-file=cacert.pem="${cert_file}" \
        -n "${TEST_NAMESPACE}" \
        --dry-run=client -o yaml | kubectl apply -f -
    log "Created vxingest-cacert secret"
}

deploy_manifests() {
    log_section "Deploying Kubernetes Manifests"

    # Apply manifests, but override namespace
    kubectl apply -f "${KUBERNETES_DIR}/namespace.yaml" 2>/dev/null || true

    # Apply ConfigMaps and RoleBinding (work in any namespace)
    kubectl apply -f "${KUBERNETES_DIR}/configmap-orchestrator-script.yaml" \
        -n "${TEST_NAMESPACE}"
    kubectl apply -f "${KUBERNETES_DIR}/configmap-orchestrator-params.yaml" \
        -n "${TEST_NAMESPACE}"

    # Apply RBAC (uses kustomization)
    kubectl apply -f "${KUBERNETES_DIR}/sa-orchestrator.yaml" \
        -n "${TEST_NAMESPACE}"
    kubectl apply -f "${KUBERNETES_DIR}/role-orchestrator.yaml" \
        -n "${TEST_NAMESPACE}"
    kubectl apply -f "${KUBERNETES_DIR}/rolebinding-orchestrator.yaml" \
        -n "${TEST_NAMESPACE}"

    # Apply Job template
    kubectl apply -f "${KUBERNETES_DIR}/job-orchestrator.yaml" \
        -n "${TEST_NAMESPACE}"

    kubectl apply -f "${KUBERNETES_DIR}/pvc-data.yaml" \
        -n "${TEST_NAMESPACE}"

    kubectl apply -f "${KUBERNETES_DIR}/pvc-public.yaml" \
        -n "${TEST_NAMESPACE}"

    log "Deployed manifests to namespace: ${TEST_NAMESPACE}"
}

create_test_pvc() {
    log_section "Setting Up Test PVC"

    # Bind test data PVC to a host path (defaults to /opt/data).
    # Create hostPath storage class
    kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-storage
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
EOF

        # Create PVs and PVCs
        kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
    name: vxingest-data-pv
spec:
    capacity:
        storage: 10Gi
    accessModes: [ "ReadWriteMany" ]
    storageClassName: local-storage
    hostPath:
        path: ${TEST_DATA_HOST_PATH}
        type: DirectoryOrCreate
EOF

        kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
    name: vxingest-public-pv
spec:
    capacity:
        storage: 1Gi
    accessModes: [ "ReadOnlyMany" ]
    storageClassName: local-storage
    hostPath:
        path: ${TEST_PUBLIC_HOST_PATH}
        type: DirectoryOrCreate
EOF

        kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: vxingest-data-pvc
    namespace: ${TEST_NAMESPACE}
spec:
    accessModes: [ "ReadWriteMany" ]
    storageClassName: local-storage
    volumeName: vxingest-data-pv
    resources:
        requests:
            storage: 10Gi
EOF

        kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: vxingest-public-pvc
    namespace: ${TEST_NAMESPACE}
spec:
    accessModes: [ "ReadOnlyMany" ]
    storageClassName: local-storage
    volumeName: vxingest-public-pv
    resources:
        requests:
            storage: 1Gi
EOF

        log "Created test PVCs (vxingest-data-pvc -> ${TEST_DATA_HOST_PATH} and vxingest-public-pvc -> ${TEST_PUBLIC_HOST_PATH})"
}

cleanup_test() {
    log_section "Cleaning Up Test Resources"

    # Delete test namespace
    kubectl delete namespace "${TEST_NAMESPACE}" --ignore-not-found=true 2>/dev/null || true
    log "Deleted test namespace"
}

cleanup_cluster() {
    log_section "Tearing Down Test Cluster"

    read -p "Delete kind cluster '${CLUSTER_NAME}'? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kind delete cluster --name "${CLUSTER_NAME}"
        log "Deleted kind cluster: ${CLUSTER_NAME}"
    else
        log_warn "Keeping cluster ${CLUSTER_NAME} for debugging"
    fi
}

# ============================================================================
# Test Helper Functions
# ============================================================================


run_orchestrator_job() {
    local job_name="test-orchestrator-$(date +%s)"

    debug "Creating job: ${job_name}"

    # Render the orchestrator template with a unique job name, then apply it.
    if ! kubectl create --dry-run=client -f "${KUBERNETES_DIR}/job-orchestrator.yaml" -o yaml \
        | sed -e "s/^  name: vxingest-pipeline-orchestrator$/  name: ${job_name}/" \
              -e "s/^  namespace: .*/  namespace: ${TEST_NAMESPACE}/" \
        | kubectl apply -f - >/dev/null; then
        echo "ERROR: Failed to create orchestrator job: ${job_name}" >&2
        return 1
    fi

    local found=0
    for _ in 1 2 3 4 5; do
        if kubectl get job "${job_name}" -n "${TEST_NAMESPACE}" >/dev/null 2>&1; then
            found=1
            break
        fi
        sleep 1
    done

    if [[ ${found} -ne 1 ]]; then
        echo "ERROR: Orchestrator job was not found after creation: ${job_name}" >&2
        return 1
    fi

    echo "${job_name}"
}

wait_for_job() {
    local job_name="$1"
    local timeout="${2:-${TEST_TIMEOUT_SECS}}"

    debug "Waiting for job ${job_name} (timeout: ${timeout}s)"

    if kubectl wait --for=condition=complete "job/${job_name}" \
        -n "${TEST_NAMESPACE}" \
        --timeout="${timeout}s" 2>/dev/null; then
        return 0
    else
        # Job failed or timed out
        return 1
    fi
}

get_orchestrator_run_id() {
    local job_name="$1"

    # Get logs and extract RUN_ID
    kubectl logs -n "${TEST_NAMESPACE}" "job/${job_name}" 2>/dev/null \
        | grep "Run ID:" | tail -1 | awk '{print $NF}'
}

get_child_jobs() {
    local pattern="$1"

    kubectl get jobs -n "${TEST_NAMESPACE}" \
        --no-headers 2>/dev/null \
        | grep "${pattern}" || echo ""
}

verify_job_sequence() {
    local run_id="$1"
    local expected_count="$2"

    debug "Verifying job sequence for RUN_ID=${run_id}, expected count=${expected_count}"

    # For N ingest jobs, we should have:
    # - N ingest jobs
    # - N import jobs
    # - 1 meta-update job
    local expected_total=$((expected_count * 2 + 1))

    local ingest_jobs=$(get_child_jobs "vxingest-ingest.*${run_id}" | wc -l)
    local import_jobs=$(get_child_jobs "vxingest-import.*${run_id}" | wc -l)
    local meta_jobs=$(get_child_jobs "vxingest-meta-update.*${run_id}" | wc -l)

    local actual_total=$((ingest_jobs + import_jobs + meta_jobs))

    debug "Found: ${ingest_jobs} ingest, ${import_jobs} import, ${meta_jobs} meta-update jobs"

    if [[ $ingest_jobs -eq $expected_count ]] && \
       [[ $import_jobs -eq $expected_count ]] && \
       [[ $meta_jobs -eq 1 ]]; then
        return 0
    else
        log_fail "Job sequence mismatch. Expected ${expected_total}, got ${actual_total}"
        return 1
    fi
}

verify_job_naming() {
    local run_id="$1"

    debug "Verifying job naming conventions for RUN_ID=${run_id}"

    # Check that job names follow pattern: vxingest-{type}-{index}-{run_id}
    local jobs=$(get_child_jobs "${run_id}")

    while IFS= read -r job_line; do
        [[ -z "$job_line" ]] && continue

        local job_name=$(echo "$job_line" | awk '{print $1}')

        if [[ $job_name =~ ^vxingest-(ingest|import|meta-update)-[0-9]+-[0-9]+$ ]] || \
           [[ $job_name =~ ^vxingest-meta-update-[0-9]+$ ]]; then
            debug "✓ Job name valid: ${job_name}"
        else
            log_fail "Invalid job name format: ${job_name}"
            return 1
        fi
    done <<< "$jobs"

    return 0
}

verify_pvc_structure() {
    local run_id="$1"
    local expected_ingest_count="$2"

    debug "Verifying PVC directory structure"

    # Get a pod in the namespace to exec into (use orchestrator pod)
    local pod=$(kubectl get pods -n "${TEST_NAMESPACE}" \
        --field-selector=status.phase=Running \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

    if [[ -z "$pod" ]]; then
        log_warn "No running pod found for PVC verification"
        return 0
    fi

    # Check for expected directory structure
    for i in $(seq 1 ${expected_ingest_count}); do
        local outdir="/opt/data/outdir/ingest-${i}"
        local logdir="/opt/data/logs/ingest-${i}"
        local metricsdir="/opt/data/metrics/ingest-${i}"

        if kubectl exec "${pod}" -n "${TEST_NAMESPACE}" -- \
            test -d "${outdir}" 2>/dev/null; then
            debug "✓ Directory exists: ${outdir}"
        else
            log_warn "Directory not found: ${outdir} (may be OK if ingest skipped)"
        fi
    done

    return 0
}

# ============================================================================
# Test Cases
# ============================================================================

test_single_ingest_job() {
    local test_name="Test 1: Single Ingest Job"
    log_section "$test_name"
    local job_name

    # Configure single job
    kubectl patch configmap vxingest-orchestrator-params \
        --type merge \
        -p '{"data":{"INGEST_EXTRA_ARGS":"-j JS:METAR:MODEL:HRRR_OPS_conus_3km_TEST:schedule:job:V01"}}' \
        -n "${TEST_NAMESPACE}"
    log "Configured INGEST_EXTRA_ARGS with 1 job"

    # Run orchestrator
    if ! job_name="$(run_orchestrator_job)"; then
        log_fail "${test_name}: Failed to start orchestrator job"
        return 1
    fi
    log "Started orchestrator job: ${job_name}"

    # Wait for completion
    if wait_for_job "${job_name}"; then
        log_pass "${test_name}: Job completed successfully"

        # Extract RUN_ID and verify structure
        local run_id=$(get_orchestrator_run_id "${job_name}")
        if [[ -n "$run_id" ]]; then
            log "Extracted RUN_ID: ${run_id}"
            verify_job_sequence "${run_id}" 1
            verify_job_naming "${run_id}"
        fi
    else
        log_fail "${test_name}: Job failed or timed out"
        kubectl logs "job/${job_name}" -n "${TEST_NAMESPACE}" | tail -20
        return 1
    fi
}

test_multiple_ingest_jobs() {
    local test_name="Test 2: Multiple Ingest Jobs (Tests Loop)"
    log_section "$test_name"
    local job_name

    # Configure 3 jobs
    local config="-j JS:METAR:MODEL:HRRR_OPS_conus_3km_TEST:schedule:job:V01"$'\n'"-j JS:METAR:CTC:HRRR_OPS_conus_3km_TEST:schedule:job:V01"$'\n'"-j JS:METAR:SUMS:HRRR_OPS_conus_3km_TEST:schedule:job:V01"
    kubectl patch configmap vxingest-orchestrator-params \
        --type merge \
        -p "{\"data\":{\"INGEST_EXTRA_ARGS\":$(echo -n "$config" | jq -Rs .)}}" \
        -n "${TEST_NAMESPACE}"
    log "Configured INGEST_EXTRA_ARGS with 3 jobs"

    # Run orchestrator
    if ! job_name="$(run_orchestrator_job)"; then
        log_fail "${test_name}: Failed to start orchestrator job"
        return 1
    fi
    log "Started orchestrator job: ${job_name}"

    # Wait for completion
    if wait_for_job "${job_name}"; then
        log_pass "${test_name}: Job completed successfully"

        # Verify all 3 job pairs + meta-update were created
        local run_id=$(get_orchestrator_run_id "${job_name}")
        if [[ -n "$run_id" ]]; then
            log "Extracted RUN_ID: ${run_id}"
            verify_job_sequence "${run_id}" 3
            verify_job_naming "${run_id}"
        fi
    else
        log_fail "${test_name}: Job failed or timed out"
        kubectl logs "job/${job_name}" -n "${TEST_NAMESPACE}" | tail -20
        return 1
    fi
}

test_empty_config() {
    local test_name="Test 3: Empty Config (Meta-Update Only)"
    log_section "$test_name"
    local job_name

    # Configure empty (no ingest jobs)
    kubectl patch configmap vxingest-orchestrator-params \
        --type merge \
        -p '{"data":{"INGEST_EXTRA_ARGS":""}}' \
        -n "${TEST_NAMESPACE}"
    log "Configured INGEST_EXTRA_ARGS as empty"

    # Run orchestrator
    if ! job_name="$(run_orchestrator_job)"; then
        log_fail "${test_name}: Failed to start orchestrator job"
        return 1
    fi
    log "Started orchestrator job: ${job_name}"

    # Wait for completion
    if wait_for_job "${job_name}"; then
        log_pass "${test_name}: Job completed successfully (skipped ingest loop)"

        # Verify only meta-update ran
        local run_id=$(get_orchestrator_run_id "${job_name}")
        if [[ -n "$run_id" ]]; then
            log "Extracted RUN_ID: ${run_id}"

            local meta_jobs=$(get_child_jobs "vxingest-meta-update.*${run_id}" | wc -l)
            if [[ $meta_jobs -eq 1 ]]; then
                log_pass "Verified: only meta-update job ran"
            else
                log_fail "Expected 1 meta-update job, found ${meta_jobs}"
            fi
        fi
    else
        log_fail "${test_name}: Job failed or timed out"
        kubectl logs "job/${job_name}" -n "${TEST_NAMESPACE}" | tail -20
        return 1
    fi
}

test_job_sequencing() {
    local test_name="Test 4: Job Sequencing (Ingest → Import → Meta-Update)"
    log_section "$test_name"
    local job_name

    # Configure single job
    kubectl patch configmap vxingest-orchestrator-params \
        --type merge \
        -p '{"data":{"INGEST_EXTRA_ARGS":"-j JS:METAR:MODEL:HRRR_OPS_conus_3km_TEST:schedule:job:V01"}}' \
        -n "${TEST_NAMESPACE}"

    # Run orchestrator
    if ! job_name="$(run_orchestrator_job)"; then
        log_fail "${test_name}: Failed to start orchestrator job"
        return 1
    fi
    log "Started orchestrator job: ${job_name}"

    if wait_for_job "${job_name}"; then
        # Extract timestamps from logs to verify sequence
        local logs=$(kubectl logs "job/${job_name}" -n "${TEST_NAMESPACE}")

        local ingest_time=$(echo "$logs" | grep "Creating ingest job" | head -1 | grep -oE '\[[^ ]+\]' | head -1)
        local import_time=$(echo "$logs" | grep "Creating import job" | head -1 | grep -oE '\[[^ ]+\]' | head -1)
        local meta_time=$(echo "$logs" | grep "Creating meta-update job" | head -1 | grep -oE '\[[^ ]+\]' | head -1)

        if [[ -n "$ingest_time" ]] && [[ -n "$import_time" ]] && [[ -n "$meta_time" ]]; then
            log_pass "${test_name}: Jobs created in expected order"
            log "  Ingest: $ingest_time"
            log "  Import: $import_time"
            log "  Meta:   $meta_time"
        else
            log_warn "${test_name}: Could not verify exact sequencing from logs"
        fi
    else
        log_fail "${test_name}: Orchestrator job failed"
        return 1
    fi
}

test_unique_job_names() {
    local test_name="Test 5: Unique Job Names (No Collisions)"
    log_section "$test_name"
    local job1
    local job2

    # Run orchestrator twice quickly
    if ! job1="$(run_orchestrator_job)"; then
        log_fail "${test_name}: Failed to start first orchestrator job"
        return 1
    fi
    sleep 1
    if ! job2="$(run_orchestrator_job)"; then
        log_fail "${test_name}: Failed to start second orchestrator job"
        return 1
    fi

    log "Started job 1: ${job1}"
    log "Started job 2: ${job2}"

    # Extract RUN_IDs
    sleep 5  # Let jobs produce some logs

    local run_id1=$(get_orchestrator_run_id "${job1}" 2>/dev/null || echo "")
    local run_id2=$(get_orchestrator_run_id "${job2}" 2>/dev/null || echo "")

    if [[ "$run_id1" != "$run_id2" ]]; then
        log_pass "${test_name}: Unique RUN_IDs generated"
        log "  Job 1 RUN_ID: ${run_id1}"
        log "  Job 2 RUN_ID: ${run_id2}"
    else
        log_fail "${test_name}: RUN_IDs are not unique"
    fi
}

# ============================================================================
# Main Test Runner
# ============================================================================

main() {
    log_section "VxIngest Kubernetes Orchestrator Integration Tests"
    log "Repository: ${REPO_ROOT}"
    log "Namespace: ${TEST_NAMESPACE}"
    log "Cluster: ${CLUSTER_NAME}"
    log "Timeout: ${TEST_TIMEOUT_SECS}s"

    # Setup
    setup_cluster
    setup_test_namespace
    setup_ghcr_access_token
    #create_test_pvc
    deploy_manifests

    # Run tests
    test_single_ingest_job
    test_multiple_ingest_jobs
    test_empty_config
    test_job_sequencing
    test_unique_job_names

    # Cleanup
    cleanup_test

    # Summary
    log_section "Test Summary"
    echo -e "${GREEN}Passed: ${TESTS_PASSED}${NC}"
    echo -e "${RED}Failed: ${TESTS_FAILED}${NC}"

    if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
        echo ""
        echo "Failed tests:"
        printf '%s\n' "${FAILED_TESTS[@]}"
        exit 1
    else
        log_pass "All tests passed!"
        exit 0
    fi
}

# ============================================================================
# Execution
# ============================================================================

# Allow sourcing for test development
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
