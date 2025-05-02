#!/bin/bash

# Define array of configurations (populateWorkload, doCache, directory)
CONFIGS=(
  "workloads/populateDB_1000 true exp_soar_cache_1000_10 workloads/ListFriendsAction"
  "workloads/populateDB_1000 false exp_soar_withoutcache_1000_10 workloads/ListFriendsAction"
  "workloads/populateDB2_1000 true exp_soar_cache_1000_100 workloads/ListFriendsAction"
  "workloads/populateDB2_1000 false exp_soar_withoutcache_1000_100 workloads/ListFriendsAction"
  "workloads/populateDB_10000 true exp_soar_cache_10000_10 workloads/ListFriendsAction2"
  "workloads/populateDB_10000 false exp_soar_withoutcache_10000_10 workloads/ListFriendsAction2"
  "workloads/populateDB2_10000 true exp_soar_cache_10000_100 workloads/ListFriendsAction2"
  "workloads/populateDB2_10000 false exp_soar_withoutcache_10000_100 workloads/ListFriendsAction2"
)

for CONFIG in "${CONFIGS[@]}"; do
  # Split string into parts
  read -r POPULATE_WORKLOAD DOCACHE DIRECTORY WORKLOAD<<< "$CONFIG"

  echo "========== Running: $POPULATE_WORKLOAD | DOCACHE=$DOCACHE | DIRECTORY=$DIRECTORY | WORKLOAD=$WORKLOAD =========="

  ./rating_pipeline.sh \
    -populateWorkload "$POPULATE_WORKLOAD" \
    -doCache "$DOCACHE" \
    -directory "$DIRECTORY" \
    -workload "$WORKLOAD"

  echo "========== Finished: $DIRECTORY =========="
  echo ""
done
