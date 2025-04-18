#!/bin/bash

# Define array of configurations (populateWorkload, doCache, directory)
CONFIGS=(
  "workloads/populateDB_1000 true exp_soar_cache_1000_10"
  "workloads/populateDB_1000_2 true exp_soar_cache_1000_100"
  "workloads/populateDB_10000 true exp_soar_cache_10000_10"
  "workloads/populateDB_10000_2 true exp_soar_cache_10000_100"
  "workloads/populateDB_1000 false exp_soar_withoutcache_1000_10"
  "workloads/populateDB_1000_2 false exp_soar_withoutcache_1000_100"
  "workloads/populateDB_10000 false exp_soar_withoutcache_10000_10"
  "workloads/populateDB_10000_2 false exp_soar_withoutcache_10000_100"
)

for CONFIG in "${CONFIGS[@]}"; do
  # Split string into parts
  read -r POPULATE_WORKLOAD DOCACHE DIRECTORY <<< "$CONFIG"

  echo "========== Running: $POPULATE_WORKLOAD | DOCACHE=$DOCACHE | DIRECTORY=$DIRECTORY =========="

  ./rating_pipeline.sh \
    -populateWorkload "$POPULATE_WORKLOAD" \
    -doCache "$DOCACHE" \
    -directory "$DIRECTORY"

  echo "========== Finished: $DIRECTORY =========="
  echo ""
done
