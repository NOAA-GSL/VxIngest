# run-import.sh

`run-import.sh` scans an import directory for `*.gz` tarballs, extracts each into a temporary folder, imports all `*.json` payloads with `cbimport`, archives each tarball with success/failure prefix, and optionally performs a metadata update pass after successful imports.

## Usage

```bash
scripts/VXingest_utilities/import/run-import.sh \
    -c /path/to/credentials \
    -l load_dir \
    -t temp_dir
```

## Container Runtime Contract

1. Base image target: Linux AMD64.
2. Entry point: `scripts/VXingest_utilities/import/run-import.sh`.
3. Credentials file is provided as a secret (typically at `/run/secrets/CREDENTIALS_FILE`).
4. The import data root is `/opt/data_import`.
5. `-l` and `-t` are subdirectory names under `/opt/data_import`.
6. The script writes logs under `/opt/data_import/logs`.

### Required Arguments

1. `-c`: Credentials file path.
2. `-l`: Load directory name under `/opt/data_import` containing inbound `*.gz` files.
3. `-t`: Temporary extraction directory name under `/opt/data_import`.

### Credentials File Keys

Required keys:

1. `cb_host`
2. `cb_user`
3. `cb_password`
4. `cb_bucket`
5. `cb_scope`
6. `cb_collection`

Accepted formatting for keys:

1. `key value`
2. `key: value`

### Runtime Expectations

1. Script is intended to run from the repository root so relative paths like `./meta_update_middleware` resolve.
2. Script enforces execution as `amb-verif`.
3. `cbimport` is expected at `${HOME}/cbtools/bin/cbimport`.
4. On macOS, `nproc` is not available by default; use an alias such as `alias nproc="sysctl -n hw.ncpu"`.

### Processing Flow

For each `*.gz` tarball in `load_dir`:

1. Create a per-tarball lock file (`<tar>.lock`) to avoid double-processing.
2. Extract tarball into a unique temp directory under `temp_dir`.
3. Import each JSON file with `cbimport`.
4. Move source tarball into `load_dir/archive` with status prefix.
5. Remove temp extraction directory.

### Archive Naming Conventions

Tarballs are moved to `load_dir/archive` using these prefixes:

1. `success-<name>.gz`
2. `failed-extract-<name>.gz`
3. `failed-no-json-files-<name>.gz`
4. `failed-import-<name>.gz`

### Concurrency Controls

1. Script-level throttle: refuses to run when more than 10 `run-import.sh` processes are detected.
2. File-level lock: `<tar>.lock` prevents two processes from importing the same tarball concurrently.

### Metadata Update Phase

Metadata update runs only when:

1. `update_metadata_enabled` is `true`.
2. At least one import succeeded in current run.

Behavior:

1. Uses lock directory `/data/import_lock`.
2. If lock exists and a `meta-update` process is running, metadata update is skipped.
3. If lock exists and no `meta-update` process is running, lock is treated as stale and removed.
4. Runs `./meta-update` from `./meta_update_middleware`.
5. Writes output to `/opt/data_import/logs/meta-update.log`.

### Exit and Error Behavior

1. Missing required args, invalid paths, wrong user, or Capella cert validation failure cause immediate exit via `usage`.
2. Tarball-level failures are isolated: processing continues to next tarball.
3. Metadata update failures are logged to stderr.

### Operational Notes

1. Keep `/opt/data_import/<load_dir>/archive` writable by `amb-verif`.
2. Ensure `/opt/data_import/logs` is writable for metadata update logs.
3. Review archive prefixes to monitor failure modes quickly.
4. If using Capella, confirm `cacert_file` contains a PEM certificate (`BEGIN CERTIFICATE`).
