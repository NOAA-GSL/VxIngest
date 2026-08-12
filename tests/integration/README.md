# VxIngest Kubernetes Integration Tests

This directory contains integration tests for the Kubernetes orchestrator workflow.

## Quick Start

### Prerequisites

- `docker` for kind
- `kind`
- `kubectl`
- `openssl`

Install kind if needed:

```bash
go install sigs.k8s.io/kind@latest
```

Run the harness from the repository root:

```bash
bash tests/integration/test-orchestrator.sh
VERBOSE=1 bash tests/integration/test-orchestrator.sh
```

## What Gets Tested

The harness checks:

- single-job orchestration
- multi-job orchestration loops
- empty `JOBIDS` handling
- child job sequencing
- RUN_ID uniqueness

Expected child jobs per orchestrator run:

- `vxingest-ingest-N-<run_id>`
- `vxingest-import-N-<run_id>`
- `vxingest-meta-update-<run_id>`

## Current Scope

This harness documents and tests the current orchestrator script in `kubernetes/configmap-job-script.yaml`. That script still creates downstream import and meta-update child jobs using external images, even though those runtimes are no longer maintained in this branch.

## Environment Variables

```bash
VERBOSE=1
CLUSTER_NAME=my-cluster
TEST_DATA_HOST_PATH=/opt/data
TEST_PUBLIC_HOST_PATH=/opt/data
```

## Debugging Failed Tests

View orchestrator logs:

```bash
kubectl logs job/test-orchestrator-<timestamp> -n vxingest-test
```

Check created jobs:

```bash
kubectl get jobs -n vxingest-test
```

Inspect a child job:

```bash
kubectl describe job vxingest-ingest-1-<run_id> -n vxingest-test
kubectl logs job/vxingest-ingest-1-<run_id> -n vxingest-test
```

Keep the cluster for manual debugging by answering `n` when the harness asks whether to delete the kind cluster.

## Adding New Tests

The harness is modular. Add a new shell function and call it from `main()` in `tests/integration/test-orchestrator.sh`.

## CI status

These tests are currently intended for manual execution. This branch does not include a dedicated GitHub Actions workflow for `tests/integration/test-orchestrator.sh`.

## Limitations

- Tests use local secrets and host-mounted data rather than a fully isolated Couchbase test deployment.
- Ingest jobs may still fail at runtime if the provided credentials or data paths are invalid.
- The main focus is Kubernetes orchestration behavior, not scientific data validation.

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

### Jobs stuck in Pending

```bash
kubectl get roles -n vxingest-test
kubectl get rolebindings -n vxingest-test
kubectl get pods -n vxingest-test
```

## Quick Reference

```bash
bash tests/integration/test-orchestrator.sh
VERBOSE=1 bash tests/integration/test-orchestrator.sh
kind delete cluster --name vxingest-test
```
