import argparse
import io
import json
import os
import shutil
import tarfile
import tempfile
import time
from pathlib import Path, PurePosixPath


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a merged MANIFEST from JSON files in a .tar.gz archive and insert it at the archive root."
    )
    parser.add_argument(
        "archive",
        type=Path,
        help="Path to the input .tar.gz archive file",
    )
    parser.add_argument(
        "--manifest-name",
        type=str,
        default="MANIFEST",
        help="Entry name for the manifest at the archive root (default: 'MANIFEST')",
    )
    return parser.parse_args()


def insert_manifest_into_archive(archive_path: Path, manifest_name: str) -> None:
    if not archive_path.is_file():
        raise FileNotFoundError(f"Archive not found: {archive_path}")

    records = []
    processed_files = 0

    # Pass 1: Stream through the archive to build the manifest in memory
    with tarfile.open(archive_path, mode="r:gz") as tar:
        for member in tar:
            if not member.isfile():
                continue

            path_parts = [p for p in PurePosixPath(member.name).parts if p != "."]
            if not path_parts:
                continue

            filename = path_parts[-1]

            # Exclude any path containing a "data" directory or a file named "data" / "data.json"
            if any(part == "data" or Path(part).stem == "data" for part in path_parts):
                continue

            # Ignore "LJ:" files, macOS metadata (._*), and any pre-existing root MANIFEST entry
            if (
                not filename.endswith(".json")
                or filename.startswith("LJ:")
                or filename.startswith("._")
                or PurePosixPath(member.name).as_posix().lstrip("./") in {manifest_name, f"{manifest_name}.json"}
            ):
                continue

            extracted = tar.extractfile(member)
            if extracted is not None:
                parsed = json.load(extracted)

                if isinstance(parsed, dict):
                    records.append({k: v for k, v in parsed.items() if k != "data"})
                    processed_files += 1
                elif isinstance(parsed, list):
                    dict_items = [
                        {k: v for k, v in item.items() if k != "data"}
                        for item in parsed
                        if isinstance(item, dict)
                    ]
                    if dict_items:
                        records.extend(dict_items)
                        processed_files += 1
                    else:
                        print(f"Skipping non-object JSON array in: {member.name}")
                else:
                    print(f"Skipping primitive JSON value in: {member.name}")

    if not records:
        raise FileNotFoundError(f"No valid JSON object records found inside {archive_path}")

    # Collect all top-level keys in order of appearance
    all_keys = list(dict.fromkeys(k for rec in records for k in rec))

    manifest = {}
    for key in all_keys:
        unique_serialized = {}
        for rec in records:
            if key in rec:
                val = rec[key]
                serialized = json.dumps(val, sort_keys=True)
                if serialized not in unique_serialized:
                    unique_serialized[serialized] = val

        unique_values = list(unique_serialized.values())

        if len(unique_values) == 1 and all(key in rec for rec in records):
            manifest[key] = unique_values[0]
        else:
            manifest[key] = unique_values

    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    target_arcname = manifest_name

    # Pass 2: Stream existing entries + new root 'manifest' into a temp .tar.gz, then atomically replace
    fd, tmp_name = tempfile.mkstemp(dir=archive_path.parent, suffix=".tmp.tar.gz")
    os.close(fd)
    tmp_path = Path(tmp_name)

    try:
        with (
            tarfile.open(archive_path, mode="r:gz") as src_tar,
            tarfile.open(tmp_path, mode="w:gz") as dst_tar,
        ):
            for member in src_tar:
                clean_name = PurePosixPath(member.name).as_posix().lstrip("./")
                # Skip any existing root manifest entry so re-running updates it cleanly without duplicates
                if clean_name == target_arcname:
                    continue

                if member.isfile():
                    file_obj = src_tar.extractfile(member)
                    dst_tar.addfile(member, fileobj=file_obj)
                else:
                    dst_tar.addfile(member)

            # Create TarInfo header for the new root 'manifest' file
            info = tarfile.TarInfo(name=target_arcname)
            info.size = len(manifest_bytes)
            info.mtime = int(time.time())
            info.mode = 0o644
            dst_tar.addfile(info, fileobj=io.BytesIO(manifest_bytes))

        # Preserve original file permissions and atomically overwrite archive
        shutil.copymode(archive_path, tmp_path)
        tmp_path.replace(archive_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    print(
        f"Streamed {processed_files} files ({len(records)} records) and inserted "
        f"'{target_arcname}' at root of {archive_path.name}"
    )


def main() -> None:
    args = parse_args()
    archive_path = args.archive.expanduser().resolve()
    insert_manifest_into_archive(archive_path, args.manifest_name)


if __name__ == "__main__":
    main()
