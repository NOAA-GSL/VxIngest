# AGENTS.md — VxIngest

Instructions for AI coding agents working in this repository.

## Project overview

VxIngest is a two-stage pipeline for ingesting meteorological verification data into Couchbase:

1. **Ingest** (Python) — Reads GRIB2, NetCDF, or Couchbase source data, transforms it into Couchbase-ready JSON documents, and writes them to disk along with Prometheus metrics and logs.
2. **Import** (shell) — A bash wrapper around `cbimport` (`scripts/VXingest_utilities/import/run-import.sh`) that loads the JSON documents into Couchbase.

The entrypoint is `src/vxingest/main.py`, which discovers job documents from Couchbase, selects which to run, and dispatches to subtype-specific builder classes.

## Repository layout

```text
src/vxingest/                       # Main package (src-layout)
├── main.py                         # Entrypoint: CLI parsing, job discovery, dispatch
├── log_config.py                   # Multiprocessing-safe logging via QueueListener
├── builder_common/                 # Shared builder infrastructure
│   ├── builder.py                  # Abstract Builder base class
│   ├── builder_utilities.py        # Shared helper functions
│   ├── vx_ingest.py                # CommonVxIngest — Couchbase connection & thread pool
│   └── ingest_manager.py           # Base ingest manager for worker threads
├── grib2_to_cb/                    # GRIB2 → Couchbase
│   ├── run_ingest_threads.py       # VXIngest.runit() entry point
│   ├── grib_builder.py             # GribModelBuilderV01
│   ├── grib_builder_parent.py      # Parent class for GRIB builders
│   └── vx_ingest_manager.py        # Worker thread manager
├── netcdf_to_cb/                   # NetCDF → Couchbase
│   ├── run_ingest_threads.py       # VXIngest.runit() entry point
│   ├── netcdf_metar_obs_builder.py # METAR observation builder
│   ├── netcdf_tropoe_obs_builder.py# TROPOE observation builder
│   └── vx_ingest_manager.py        # Worker thread manager
├── ctc_to_cb/                      # Contingency Table Counts (Couchbase → Couchbase)
│   ├── run_ingest_threads.py       # VXIngest.runit() entry point
│   ├── ctc_builder.py              # CTC calculation builder
│   └── vx_ingest_manager.py        # Worker thread manager
├── partial_sums_to_cb/             # Partial Sums (Couchbase → Couchbase)
│   ├── run_ingest_threads.py       # VXIngest.runit() entry point
│   ├── partial_sums_builder.py     # Partial sums calculation
│   └── vx_ingest_manager.py        # Worker thread manager
├── prepbufr_to_cb/                 # PREPBUFR (not yet enabled)
└── utilities/                      # One-off utility scripts

tests/vxingest/                     # Tests (mirrors src structure)
├── test_int_main.py                # Integration test for main dispatch
├── builder_common/                 # Builder utility tests
├── grib2_to_cb/                    # GRIB2 unit + integration tests
├── netcdf_to_cb/                   # NetCDF unit + integration tests
├── ctc_to_cb/                      # CTC unit + integration tests
└── partial_sums_to_cb/             # Partial sums tests

docs/                               # Documentation
├── decisions/                      # Architecture & Scientific Decision Records
│   ├── README.md                   # ADR/SDR process & format guide
│   ├── architecture/               # Architecture Decision Records (ADR-NNNN)
│   └── scientific/                 # Scientific Decision Records (SDR-NNNN)
├── development-guide.md            # Developer setup & workflow reference
├── couchbase.md                    # Couchbase schema & concepts
└── model/                          # Data model documentation

scripts/VXingest_utilities/         # Shell wrappers
├── import/
│   └── run-import.sh               # cbimport wrapper (watches for tarballs, loads JSON)
├── run-ingest.sh                   # Orchestrates ingest jobs
└── scrape_metrics.sh               # Parses logs for Prometheus metrics

docker/                             # Dockerfiles for ingest & import containers
compose.yaml                       # Docker Compose: ingest, import, test, shell services
cb-schemas/                         # Couchbase schema definitions
cb-samples/                         # Sample Couchbase documents (COMMON, METAR, RUNTIME)
third_party/                        # Platform-specific wheels (NCEPLIBS-bufr)
pyproject.toml                      # Project metadata, deps, tool configs, entry points
```

## Architecture & data flow

### Dispatch

`main.py` discovers job documents, builds a config dict, and dispatches via a `match` statement on `subType`:

```python
match proc["subType"]:
    case "GRIB2" | "GRIB2-TEST":     → grib2_to_cb
    case "NETCDF" | "NETCDF-TEST":   → netcdf_to_cb
    case "CTC" | "CTC-TEST":         → ctc_to_cb
    case "PARTIAL_SUMS" | "PARTIAL_SUMS-TEST": → partial_sums_to_cb
```

### Builder hierarchy

```
CommonVxIngest (builder_common/vx_ingest.py)
  └── VXIngest (per-builder run_ingest_threads.py)  — owns runit(), Couchbase connection, file queue
        └── VxIngestManager (per-builder vx_ingest_manager.py)  — worker threads processing individual files
              └── Builder subclass (e.g., GribModelBuilderV01)  — data transformation logic
```

Every builder exports `run_ingest_threads.VXIngest.runit(config, log_queue, log_configurer)`.

### Job document flows

Two coexisting flows must both be preserved:

- **Legacy**: `JOB` / `JOB-TEST` documents in the `COMMON` collection, with cron-style schedule fields. `VXINGEST_IGNORE_JOB_SCHEDULE` env var bypasses the schedule check.
- **Runtime**: `JS:` / `PS:` documents in the `RUNTIME` collection, carrying `processSpecIds` and `ingestDocumentIds`.

### Output pipeline

1. Builder writes JSON documents to `output_dir`.
2. `main.py` creates a gzip tarball of `output_dir` and places it in `transfer_dir`.
3. Prometheus `.prom` metrics are written to `metrics_dir`.
4. The import stage (`run-import.sh`) watches `transfer_dir`, extracts tarballs, and runs `cbimport`.

### Planned: AWS event-driven architecture

The system is migrating to an event-driven model on AWS:

- **Storage**: Raw GRIB2/NetCDF inputs and output tarballs move to separate **S3 buckets** (replacing local filesystem paths).
- **Triggering**: S3 events flow through **SNS/SQS** to trigger ingest and import jobs.
- **Compute**: Applications deploy on **EKS**, with **KEDA** scaling workers based on SQS queue depth.
- **Flow**: S3 object created → SNS → SQS → KEDA scales EKS pod → ingest reads from S3, writes tarball back to S3 → import triggered similarly.

When working on this migration, keep the core transformation logic (builders) decoupled from I/O so the same code works with both local filesystem and S3 backends. Prefer abstracting storage access rather than scattering S3-specific code throughout builders.

**Future consideration**: Parquet/Iceberg is being explored as an intermediate data format between ingest and downstream consumers. This is early-stage — no implementation yet, but keep it in mind when designing data output interfaces.

### Planned: Couchbase document schemas & Pydantic models

Couchbase documents (JOB, MD, DD, DF, etc.) are currently untyped dicts. The plan is to:

1. **Capture document structures as JSON Schema** — formalize the shape of each document type. These would eventually live in a `schemas` directory.
2. **Generate Pydantic models** from those schemas — providing runtime validation, type safety, and IDE autocompletion throughout the codebase.

When working on builders or document handling, prefer structured access patterns (named fields, typed dicts) over raw `dict["key"]` lookups where practical. This makes the eventual migration to Pydantic models smoother.

## Logging & metrics

- **Logging** uses a multiprocessing-safe queue pattern (`log_config.py`). The main process creates a `QueueListener`; worker processes call `worker_log_configurer(queue)` to route logs through the queue. Do not break this pattern — multiprocessing uses `spawn`, so file handles and connections cannot be shared directly.
- **Metrics** are emitted via `prometheus-client` as Gauge/Counter values written to `.prom` files. Key metrics: `run_ingest_duration`, `run_ingest_success_count`, `run_ingest_failure_count`, `job_last_success_unixtime`.

## Developer workflows

All tooling runs through [uv](https://docs.astral.sh/uv/). See `docs/development-guide.md` for full setup instructions.

```bash
# Run ingest locally
uv run ingest -m tmp/output/metrics -o tmp/output/out -x tmp/output/xfer \
    -l tmp/output/log -c config.yaml -j JOB-TEST:V01:METAR:NETCDF:OBS

# Lint & format
uv run ruff check .
uv run ruff format .

# Type check
uv run mypy src

# Run tests (requires CREDENTIALS env var pointing to a config.yaml with Couchbase creds)
CREDENTIALS=config.yaml uv run pytest tests

# Skip integration tests (no Couchbase needed)
CREDENTIALS=config.yaml uv run pytest -m "not integration" tests

# Coverage
CREDENTIALS=config.yaml uv run coverage run -m pytest tests && uv run coverage report
```

**Docker**: `compose.yaml` defines `ingest`, `import`, `test`, and `shell` services. Secrets are mounted via `CREDENTIALS_FILE`.

**Credentials** (in `config.yaml`):

```yaml
cb_host: "couchbase://hostname"
cb_user: "username"
cb_password: "password"
cb_bucket: "vxdata"
cb_scope: "_default"
cb_collection: "METAR"
```

## Code style & philosophy

**Write Pythonic, readable, maintainable code.** This is the primary quality standard for all contributions.

- **Readability first.** Code should be self-documenting. Prefer clear variable and function names and straightforward control flow over clever one-liners or premature abstractions.
- **Pythonic idioms.** Use list comprehensions, context managers, generators, `pathlib`, and other Python idioms where they improve clarity — not where they obscure intent.
- **Simple over complex.** Favor flat code over deeply nested structures. Keep functions focused and short. If a function is hard to name, it's probably doing too much.
- **No global state.** Multiprocessing uses `spawn`; pass dependencies (log queues, configs, connections) explicitly.
- **`src`-layout.** All imports are under `vxingest.*`.
- **Python 3.13+** — use modern language features where appropriate.

### Tooling

Configured in `pyproject.toml`:

- **Ruff** for linting and formatting.
  - Lint rules: `E`, `W`, `F`, `UP`, `I`, `PTH`, `PT`, `B`, `SIM`, `LOG`
  - Ignored: `E501`, `W505` (line length)
  - Output format: `pylint`
- **mypy** for type checking.
- **pytest** for testing (markers: `integration`).

## Decision records

Architecture and scientific decisions are recorded in `docs/decisions/` using the ADR/SDR format:

- `docs/decisions/architecture/` — Architecture Decision Records (e.g., handler patterns, naming conventions)
- `docs/decisions/scientific/` — Scientific Decision Records (e.g., height recalculation, humidity handling)

Each record follows the format: **Title**, **Status**, **Context**, **Decision**, **Consequences**. See `.github/ISSUE_TEMPLATE/{5-adr,6-sdr}-template.yaml` for the official style.

**Before making changes** that affect architecture or scientific algorithms, consult existing ADRs and SDRs. When making significant new decisions, propose a new record following the process in `docs/decisions/README.md`.

## Testing strategy

### Current state

The test suite is heavily weighted toward **integration tests** that require a live Couchbase instance and external data files. Unit tests exist but are fewer in number. This means the full test suite is expensive to run and difficult to use during rapid development.

### Future direction

New code should be designed for **testability from the start**:

- **Separate I/O from logic.** Keep data transformation in pure functions that can be tested without database connections or file system access.
- **Use dependency injection** for Couchbase connections, file readers, and other external resources. Accept these as parameters rather than constructing them internally.
- **Favor pure functions.** Functions that take inputs and return outputs (without side effects) are trivially testable.
- **Keep builders thin.** Move complex logic into well-named helper functions that can be unit-tested independently.

The goal is to enable a workflow where most logic can be verified with fast, isolated unit tests, reserving integration tests for end-to-end validation.

### Conventions

- **Unit tests**: Prefixed `test_unit_*` — no external dependencies, fast, isolated.
- **Integration tests**: Prefixed `test_int_*` — require Couchbase and/or external data, marked with `@pytest.mark.integration`.
- **Test data**: Small fixtures live in `testdata/` directories (checked in). Large data files are external (typically at `/opt/data`).
- **Skip integration tests**: `pytest -m "not integration"`.

## Guardrails for AI edits

1. **Preserve both job flows.** Do not change Couchbase query semantics or scheduling logic in `main.py` without maintaining both legacy `JOB`/`JOB-TEST` and runtime `JS:`/`PS:` flows.
2. **Don't break logging or metrics.** Preserve the multiprocessing log queue pattern, Prometheus metrics emission, tarball creation, and output directory structure.
3. **Register new ingest types properly.** When adding a new builder: create the directory under `src/vxingest/`, implement `run_ingest_threads.VXIngest.runit()`, and add the `case` to the dispatch `match` in `main.py`.
4. **Update `pyproject.toml` for new dependencies.** Keep platform-specific wheels in `third_party/` only when unavoidable.
5. **Write testable code.** Design new code so it can be unit-tested. Separate pure logic from I/O and Couchbase operations.
6. **Consult decision records.** Check `docs/decisions/` before making changes that touch architecture or scientific algorithm choices. Propose new ADRs/SDRs for significant decisions.
7. **Keep it Pythonic.** Prefer readable, idiomatic Python. Don't introduce unnecessary abstractions, over-engineered patterns, or non-standard conventions.
8. **Avoid global state.** Pass dependencies explicitly — no module-level singletons or mutable globals.
9. **Keep documentation current.** When making changes that affect architecture, workflows, configuration, or public interfaces, update the relevant documentation (`AGENTS.md`, `docs/`, `README.md`) in the same PR. Documentation that drifts from the code is worse than no documentation.
