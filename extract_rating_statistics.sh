#!/bin/bash

DIRECTORY="${1:-exp4}"
output_file="$DIRECTORY/"$DIRECTORY"_statistics.txt"
> "$output_file"  # Clear the file if exists

for file in "$DIRECTORY"/BGMainClass*.log; do
  if [[ -f "$file" ]]; then
    echo "=== File: $file ===" >> "$output_file"

    # Extract thread count
    thread_count=$(grep -oP "BG Client: ThreadCount =\K\d+" "$file")
    echo "ThreadCount: $thread_count" >> "$output_file"

    # Extract from '[OVERALL], RunTime(ms)' to '[PENDING]NumOperations='
    awk '/\[OVERALL\], RunTime\(ms\)/,/\[PENDING\]NumOperations=/' "$file" >> "$output_file"

    echo "" >> "$output_file"
  fi
done
