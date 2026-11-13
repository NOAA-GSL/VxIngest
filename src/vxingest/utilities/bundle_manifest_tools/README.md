# Bundle Manifest Tools

These standalone utilities generate a `MANIFEST` entry from JSON documents in
a gzip-compressed tar archive (`.tar.gz`) and insert it at the archive root.
They update the archive in place. The ingest entry point does not currently
invoke either utility.

## Typical archive layout

A bundle commonly contains a timestamped directory with JSON documents, a load
job (LJ) document, and an ingest log, plus a root-level manifest:

```text
20260820145700/
  IS:METAR:SUMS:SURFACE:RRFSv2x:ALL_HRRR:ingest:V01.json
  LJ:METAR:vxingest.partial_sums_to_cb.run_ingest_threads:VXIngest:1787237824.json
  PS_METAR_PARTIAL__SUMS_SURFACE_RRFSv2x_V01-2026-08-20T14:57:00.log
MANIFEST
```

The timestamped directory and the listed filenames are examples, not required
by the utilities. They scan eligible JSON entries throughout the archive and
write the generated manifest as a root-level `MANIFEST` entry by default.

## Manifest contents

The tools collect top-level keys from every eligible JSON object; they do not
select fields based on a schema or treat particular fields specially. The
top-level `data` key is omitted. For each remaining key:

- If every object contains the key and all values are the same, the manifest
  stores that value directly.
- Otherwise, it stores an array of the distinct values encountered. If the key
  is absent from some objects, it is still represented as an array.

Objects inside a JSON array are processed as individual records. Non-object
items in arrays and top-level primitive JSON values are ignored. Eligible files
must be regular archive entries with a case-sensitive `.json` suffix. Entries
whose path contains a component named `data` or with stem `data`, whose filename
starts with `LJ:`, or whose filename starts with `._` are skipped. Invalid JSON
in an eligible file causes an error.

The default root entry name is `MANIFEST`. Use `--manifest-name` to choose a
different name. An existing root entry with that exact name is replaced; other
entries are retained.

## Run a utility

Run these commands from the repository root, substituting the path to the
archive you want to update.

### Python

Requires Python 3.8 or newer.

```bash
python3 src/vxingest/utilities/bundle_manifest_tools/python/create_manifest_from_bundle.py /path/to/archive.tar.gz
```

### Go

Requires Go 1.18 or newer.

```bash
go run src/vxingest/utilities/bundle_manifest_tools/go/main.go /path/to/archive.tar.gz
```

Both tools also accept `--manifest-name NAME` before the archive path. They
rewrite the archive through a temporary file and replace the original after
successful completion.

## Inspect the manifest

Use `tar` and `jq` to stream the root-level `MANIFEST` entry without extracting
the rest of the archive:

```bash
ARCHIVE=/path/to/archive.tar.gz
tar -xzOf "$ARCHIVE" MANIFEST | jq .
```

List the top-level keys:

```bash
tar -xzOf "$ARCHIVE" MANIFEST | jq 'keys'
```

list the value of a specific key:

```bash
tar -xzOf "$ARCHIVE" MANIFEST | jq key
```

Show fields represented as arrays:

```bash
tar -xzOf "$ARCHIVE" MANIFEST | jq 'with_entries(select(.value | type == "array"))'
```

Show fields represented as scalar values or objects:

```bash
tar -xzOf "$ARCHIVE" MANIFEST | jq 'with_entries(select(.value | type != "array"))'
```
