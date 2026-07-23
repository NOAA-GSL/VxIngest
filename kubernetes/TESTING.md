# Running Kubernetes Integration Tests

## Quick Start

```bash
# From repository root
cd /Users/randy.pierce/VxIngest

# Run all integration tests (creates/destroys kind cluster automatically)
bash tests/integration/test-orchestrator.sh

# Run with verbose output
VERBOSE=1 bash tests/integration/test-orchestrator.sh
```

## What Tests Check

1. **Single Ingest Job** — verifies basic orchestrator operation
2. **Multiple Ingest Jobs** — tests the dynamic loop with 3 jobs
3. **Empty Config** — ensures meta-update runs when no ingest jobs configured
4. **Job Sequencing** — validates ingest → import → meta-update order
5. **Unique Job Names** — checks RUN_ID uniqueness between runs

## Key Test Scenarios

| Scenario                             | Tests                     | Expected Result                       |
| ------------------------------------ | ------------------------- | ------------------------------------- |
| `JOBIDS="-j JOB:V01"`                | Single job loop iteration | 1 ingest-1, 1 import-1, 1 meta-update |
| `JOBIDS="-j JOB1\n-j JOB2\n-j JOB3"` | Dynamic loop              | 3 ingest, 3 import, 1 meta-update     |
| `JOBIDS=""`                          | Empty config              | Skip loop, run only meta-update       |
| Concurrent runs                      | RUN_ID uniqueness         | Different RUN_IDs, no job collisions  |

## Files Created

```text
tests/integration/
├── test-orchestrator.sh      # Main test harness (executable)
└── README.md                 # Detailed test documentation

.github/workflows/
└── k8s-integration-tests.yml # CI/CD automation (GitHub Actions)
```

## For CI/CD

The workflow automatically runs on:

- Pull requests modifying `kubernetes/**` or `tests/integration/**`
- Pushes to `main` branch with same file changes

View results in: **Actions** tab → **Kubernetes Integration Tests**

## Debugging

```bash
# Keep cluster after tests for manual inspection
# (Script will prompt before deletion)
bash tests/integration/test-orchestrator.sh

# View orchestrator logs
kubectl logs job/test-orchestrator-<timestamp> -n vxingest-test

# Check created jobs
kubectl get jobs -n vxingest-test

# Inspect specific job
kubectl describe job vxingest-ingest-1-<run_id> -n vxingest-test
```

See [tests/integration/README.md](../../tests/integration/README.md) for complete documentation.
