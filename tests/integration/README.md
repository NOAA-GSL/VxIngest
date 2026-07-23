# VxIngest Kubernetes Integration Tests

This directory contains integration tests for the VxIngest Kubernetes orchestrator workflow.

## Quick Start

### Prerequisites

- `docker` (for kind)
- `kind` (local Kubernetes cluster)
- `kubectl`
- `openssl` (for test certificate generation)

**Install kind:**

```bash
go install sigs.k8s.io/kind@latest
```

### Run All Tests

```bash
cd /Users/randy.pierce/VxIngest

# Run with automatic cluster creation/deletion
bash tests/integration/test-orchestrator.sh

# Run with verbose output for debugging
VERBOSE=1 bash tests/integration/test-orchestrator.sh

# Keep cluster after tests (useful for debugging)
KEEP_CLUSTER=1 bash tests/integration/test-orchestrator.sh
```

## What Gets Tested

### Test 1: Single Ingest Job

- Verifies orchestrator handles a single ingest job correctly
- Checks that ingest-1 → import-1 → meta-update sequence completes
- Validates job naming convention

**Expected behavior:**

- 1 ingest job (vxingest-ingest-1-{RUN_ID})
- 1 import job (vxingest-import-1-{RUN_ID})
- 1 meta-update job (vxingest-meta-update-{RUN_ID})

### Test 2: Multiple Ingest Jobs (Tests Dynamic Loop)

- Verifies orchestrator loop handles 3 ingest jobs
- Checks that each ingest-N/import-N pair runs in sequence
- **Most important test** — validates the refactored loop logic

**Expected behavior:**

- 3 ingest jobs (vxingest-ingest-1-{RUN_ID}, ...ingest-2..., ...ingest-3...)
- 3 import jobs (matching imports)
- 1 meta-update job
- All jobs complete successfully in order

### Test 3: Empty Config (Meta-Update Only)

- Verifies orchestrator handles empty INGEST_EXTRA_ARGS
- Skips the ingest loop entirely
- Runs meta-update as the only operation

**Expected behavior:**

- Loop is skipped
- Only meta-update job runs
- Pipeline completes without errors

### Test 4: Job Sequencing

- Verifies jobs execute in correct order: ingest → import → meta-update
- Checks timestamps in logs to confirm sequencing
- Validates `wait_for_job_completion()` delays

### Test 5: Unique Job Names

- Verifies each orchestrator run generates unique RUN_IDs
- Prevents job name collisions between concurrent runs
- Tests timestamp-based uniqueness

## Test Harness Structure

``` text
test-orchestrator.sh
├── Configuration
│   └── Cluster name, namespace, timeouts
├── Logging Functions
│   └── Colored output for test results
├── Setup/Teardown
│   ├── setup_cluster()
│   ├── setup_test_namespace()
│   ├── deploy_manifests()
│   └── cleanup_test()
├── Test Helpers
│   ├── run_orchestrator_job()
│   ├── wait_for_job()
│   ├── verify_job_sequence()
│   └── verify_job_naming()
└── Test Cases
    ├── test_single_ingest_job()
    ├── test_multiple_ingest_jobs()
    ├── test_empty_config()
    ├── test_job_sequencing()
    └── test_unique_job_names()
```

## Environment Variables

```bash
# Enable verbose debug output
VERBOSE=1

# Use specific kind cluster name (default: vxingest-test)
CLUSTER_NAME=my-cluster

# Keep kind cluster after tests (manual cleanup)
# (Script will ask before deleting by default)
```

## Debugging Failed Tests

### View orchestrator logs

```bash
kubectl logs job/test-orchestrator-<timestamp> -n vxingest-test
```

### Check created jobs

```bash
kubectl get jobs -n vxingest-test
```

### Inspect job details

```bash
kubectl describe job vxingest-ingest-1-<run_id> -n vxingest-test
kubectl logs job/vxingest-ingest-1-<run_id> -n vxingest-test
```

### Keep cluster for manual debugging

```bash
# Run tests and keep cluster
VERBOSE=1 bash tests/integration/test-orchestrator.sh

# When prompted, choose 'n' to keep cluster
# Then manually inspect:
kubectl -n vxingest-test get all
kubectl -n vxingest-test get configmap
kubectl -n vxingest-test get secrets
```

### Check PVC contents (via pod)

```bash
# Find a pod in the namespace
POD=$(kubectl get pod -n vxingest-test -o jsonpath='{.items[0].metadata.name}')

# List PVC structure
kubectl exec $POD -n vxingest-test -- find /opt/data -type d | head -20

# Check for output files
kubectl exec $POD -n vxingest-test -- ls -lR /opt/data/outdir/
```

## Adding New Tests

The test harness is modular. To add a new test case:

1. Create a new function:

```bash
test_my_scenario() {
    local test_name="Test N: My Scenario"
    log_section "$test_name"
    
    # Configure
    kubectl patch configmap vxingest-orchestrator-params \
        --type merge \
        -p '{"data":{"INGEST_EXTRA_ARGS":"-j MY:JOB"}}' \
        -n "${TEST_NAMESPACE}"
    
    # Run
    local job_name=$(run_orchestrator_job)
    
    # Verify
    if wait_for_job "${job_name}"; then
        log_pass "${test_name}: Success"
    else
        log_fail "${test_name}: Failed"
        return 1
    fi
}
```

2. Call it from `main()`:

```bash
test_my_scenario
```

## Integration with CI/CD

### GitHub Actions

```yaml
name: Integration Test
on: [pull_request]

jobs:
  integration-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: helm/kind-action@v1.7.0
        with:
          cluster_name: vxingest-test
      
      - name: Run integration tests
        run: bash tests/integration/test-orchestrator.sh
```

### Local Development Workflow

```bash
# Make changes to orchestrator manifests
vim kubernetes/configmap-orchestrator-script.yaml

# Run tests
VERBOSE=1 bash tests/integration/test-orchestrator.sh

# If tests fail, keep cluster for debugging
# VERBOSE=1 KEEP_CLUSTER=1 bash tests/integration/test-orchestrator.sh
```

## Limitations

- Tests use **mocked credentials** (not real Couchbase)
- Ingest jobs will attempt to connect to Couchbase and may fail at runtime
- **Primary focus:** Kubernetes orchestration (job creation, sequencing, naming) — not data ingestion
- PVC contents won't have real ingest data; tests verify structure only

## Future Enhancements

- [ ] Mock Couchbase container for end-to-end testing
- [ ] Automated PVC content verification
- [ ] Timeout handling tests (trigger early job failure)
- [ ] Error recovery tests (cleanup on orchestrator failure)
- [ ] Performance benchmarks (scaling to 10+ ingest jobs)
- [ ] Concurrent orchestrator runs (verify isolation)

## Troubleshooting

### `kind: command not found`
```bash
go install sigs.k8s.io/kind@latest
export PATH=$PATH:$(go env GOPATH)/bin
```

### Kind cluster already exists
```bash
kind delete cluster --name vxingest-test
bash tests/integration/test-orchestrator.sh
```

### PVC not binding in kind
```bash
# Create local storage manually:
mkdir -p /tmp/vxingest-data
kubectl get pvc -n vxingest-test  # should show Bound
```

### Jobs stuck in Pending
```bash
# Check RBAC
kubectl get clusterrolebinding -A | grep vxingest
kubectl get roles -n vxingest-test
kubectl get rolebindings -n vxingest-test
```

## Quick Reference: Test Commands

```bash
# Single test run
bash tests/integration/test-orchestrator.sh

# Verbose with debugging
VERBOSE=1 bash tests/integration/test-orchestrator.sh

# Keep cluster after tests
VERBOSE=1 bash tests/integration/test-orchestrator.sh
# When prompted: choose 'n' to keep cluster

# Manual inspection after failed test
kubectl -n vxingest-test get all
kubectl -n vxingest-test logs job/...

# Clean up manually
kind delete cluster --name vxingest-test
```
