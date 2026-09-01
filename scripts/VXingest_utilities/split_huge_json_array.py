#!/usr/bin/env python3
"""
Split a large JSON array into smaller JSON array files.

This script takes a JSON file containing a root-level array and splits it into
multiple smaller JSON files, each containing a subset of the original array.
This is useful for importing large datasets into Couchbase via cbimport, as it
allows for:
  - Parallel imports of multiple chunks
  - Resumable imports if a chunk fails
  - Better cluster management to avoid timeouts and resource exhaustion

The script loads the entire JSON file into memory, so ensure sufficient RAM
for your dataset. For very large files (>10GB), consider streaming-based
alternatives.

Example use cases:
    A file with many small records can be split by record count, such as 100000.
    A file with fewer large records can be split by approximate file size, such as
    500MB, then imported one-by-one or in parallel with delays between imports to
    avoid overwhelming the Couchbase Capella cluster.

Usage:
        split_huge_json_array.py <input_file> <chunk_limit> [output_dir] [output_prefix]

Author: VxIngest
License: MIT
"""

import json
import sys
from pathlib import Path

SIZE_UNITS = {
    "kb": 1024,
    "mb": 1024**2,
    "gb": 1024**3,
}


def parse_chunk_limit(value):
    """
    Parse a chunk limit as either a record count or approximate byte size.

    Plain integers are treated as records per chunk. Values ending in KB, MB,
    or GB are treated as approximate output file sizes.
    """
    normalized_value = value.strip().lower()

    for suffix, multiplier in SIZE_UNITS.items():
        if normalized_value.endswith(suffix):
            number = normalized_value[: -len(suffix)]
            return "bytes", int(float(number) * multiplier)

    return "records", int(normalized_value)


def validate_arguments(args):
    """
    Validate command-line arguments.

    Args:
        args: sys.argv[1:] (command-line arguments)

    Returns:
        tuple: (input_file, chunk_mode, chunk_limit, output_prefix, output_dir)

    Raises:
        SystemExit: If arguments are invalid
    """
    if len(args) < 2:
        print(
            "Usage: split_huge_json_array.py <input_file> <chunk_limit> [output_dir] [output_prefix]"
        )
        print("")
        print("Arguments:")
        print(
            "  input_file           Path to the JSON file containing a root-level array"
        )
        print(
            "  chunk_limit          Records per file, or approximate file size: 100000, 500MB, 1GB"
        )
        print(
            "  output_dir           (Optional) Directory for output files. Default: current directory"
        )
        print(
            "  output_prefix        (Optional) Prefix for output files. Default: input_file basename"
        )
        print("")
        print("Examples:")
        print("  # Split into 100K records per file")
        print("  python3 split_huge_json_array.py data.json 100000")
        print("")
        print("  # Split into approximately 500MB files in a separate directory")
        print("  python3 split_huge_json_array.py data.json 500MB split_files")
        print("")
        print("  # Split by size with output directory and custom prefix")
        print(
            "  python3 split_huge_json_array.py data.json 500MB split_files my_chunks"
        )
        print("")
        print("Output:")
        print(
            "  Files named: <output_prefix>_chunk_001.json, <output_prefix>_chunk_002.json, ..."
        )
        sys.exit(1)

    input_file = args[0]
    try:
        chunk_mode, chunk_limit = parse_chunk_limit(args[1])
        if chunk_limit <= 0:
            raise ValueError("chunk_limit must be > 0")
    except ValueError:
        print(
            f"Error: chunk_limit must be a positive integer or size like 500MB. Got: {args[1]}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Use custom output directory if provided, otherwise use current directory
    output_dir = args[2] if len(args) > 2 else "."

    # Use custom output prefix if provided, otherwise derive from input filename
    output_prefix = args[3] if len(args) > 3 else Path(input_file).stem

    return input_file, chunk_mode, chunk_limit, output_prefix, output_dir


def load_json_array(input_file):
    """
    Load and validate a JSON array from a file.

    Args:
        input_file (str): Path to the JSON file

    Returns:
        list: The JSON array

    Raises:
        SystemExit: If file doesn't exist, isn't valid JSON, or doesn't contain an array
    """
    input_path = Path(input_file)

    if not input_path.exists():
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {input_file} ({input_path.stat().st_size / (1024**3):.2f}GB)...")

    try:
        with input_path.open() as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {input_file}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading {input_file}: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print(
            f"Error: JSON file must contain an array at the root level. Got: {type(data).__name__}",
            file=sys.stderr,
        )
        sys.exit(1)

    return data


def write_chunk(output_file, chunk):
    """Write a chunk as a valid JSON array."""
    with output_file.open("w") as out:
        json.dump(chunk, out)


def split_by_record_count(data, chunk_limit, output_path, output_prefix):
    """Split a JSON array into chunks by record count."""
    total_records = len(data)
    total_chunks = (total_records + chunk_limit - 1) // chunk_limit
    print(f"Chunk limit: {chunk_limit:,} records per file")
    print(f"Creating {total_chunks} output files...\n")

    for chunk_num, start_index in enumerate(range(0, total_records, chunk_limit), 1):
        chunk = data[start_index : start_index + chunk_limit]
        output_file = output_path / f"{output_prefix}_chunk_{chunk_num:03d}.json"

        try:
            write_chunk(output_file, chunk)
            print(
                f"  [{chunk_num:3d}/{total_chunks}] {output_file.name} ({len(chunk):,} records)"
            )
        except Exception as e:
            print(f"Error writing {output_file}: {e}", file=sys.stderr)
            sys.exit(1)

    return total_chunks


def split_by_file_size(data, chunk_limit, output_path, output_prefix):
    """Split a JSON array into chunks by approximate serialized file size."""
    chunks = []
    current_chunk = []
    current_size = 2

    for record in data:
        record_size = len(json.dumps(record).encode("utf-8"))
        separator_size = 1 if current_chunk else 0

        if current_chunk and current_size + separator_size + record_size > chunk_limit:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 2
            separator_size = 0

        current_chunk.append(record)
        current_size += separator_size + record_size

    if current_chunk:
        chunks.append(current_chunk)

    total_chunks = len(chunks)
    print(f"Chunk limit: {chunk_limit / (1024**2):,.1f}MB approximate file size")
    print(f"Creating {total_chunks} output files...\n")

    for chunk_num, chunk in enumerate(chunks, 1):
        output_file = output_path / f"{output_prefix}_chunk_{chunk_num:03d}.json"

        try:
            write_chunk(output_file, chunk)
            output_size = output_file.stat().st_size / (1024**2)
            print(
                f"  [{chunk_num:3d}/{total_chunks}] {output_file.name} ({len(chunk):,} records, {output_size:,.1f}MB)"
            )
        except Exception as e:
            print(f"Error writing {output_file}: {e}", file=sys.stderr)
            sys.exit(1)

    return total_chunks


def split_and_write(data, chunk_mode, chunk_limit, output_prefix, output_dir):
    """
    Split a JSON array into chunks and write each chunk to a separate file.

    Each output file contains a valid JSON array of up to chunk_size elements.
    Files are named: <output_prefix>_chunk_001.json, <output_prefix>_chunk_002.json, etc.

    Args:
        data (list): The JSON array to split
        chunk_size (int): Number of records per output file
        output_prefix (str): Prefix for output filenames
        output_dir (str): Directory where chunk files will be written

    Returns:
        int: Total number of chunks created
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    total_records = len(data)
    print(f"Total records: {total_records:,}")
    print(f"Output directory: {output_path.resolve()}")

    if chunk_mode == "bytes":
        return split_by_file_size(data, chunk_limit, output_path, output_prefix)

    if chunk_limit >= total_records:
        print(
            "Warning: chunk limit is greater than or equal to the total record count; "
            "only one output file will be created. Use a smaller record count or a "
            "size limit like 500MB to create multiple files."
        )

    return split_by_record_count(data, chunk_limit, output_path, output_prefix)


def main():
    """Main entry point."""
    input_file, chunk_mode, chunk_limit, output_prefix, output_dir = validate_arguments(
        sys.argv[1:]
    )
    data = load_json_array(input_file)
    total_chunks = split_and_write(
        data, chunk_mode, chunk_limit, output_prefix, output_dir
    )

    print(f"\nDone! Created {total_chunks} chunk files.")
    print("\nNext steps:")
    print("  1. Import chunks using cbimport_capella.sh:")
    print(f"     for chunk in {output_dir}/{output_prefix}_chunk_*.json; do")
    print('       ./cbimport_capella.sh -C METAR -c credentials.yaml -f "$chunk" -t 1')
    print("       sleep 10  # Wait between imports to avoid overwhelming cluster")
    print("     done")


if __name__ == "__main__":
    main()
