#!/bin/bash

# 1. Check if a directory was provided as an argument
if [ -z "$1" ]; then
    echo "Usage: $0 <input_directory>"
    exit 1
fi

INPUT_DIR="$1"

# 2. Check if the provided path is a valid directory
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: '$INPUT_DIR' is not a directory."
    exit 1
fi

# 3. Create a temporary directory
# mktemp -d ensures a unique, secure name
TEMP_DIR=$(mktemp -d)
OUTPUT_FILE="$TEMP_DIR/output.txt"

echo "Using temporary directory: $TEMP_DIR"

# 4. Start the output file with the opening bracket
echo "[" > "$OUTPUT_FILE"

# 5. Loop through each file in the input directory
# We use a null-separated glob to handle filenames with spaces safely
shopt -s nullglob
for file in "$INPUT_DIR"/*; do
    if [ -f "$file" ]; then
        # Append file content
        cat "$file" >> "$OUTPUT_FILE"
        # Append the comma
        echo "," >> "$OUTPUT_FILE"
    fi
done

# 6. Append the closing bracket
echo "]" >> "$OUTPUT_FILE"

echo "Done! Output created at: $OUTPUT_FILE"
