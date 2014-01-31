#!/bin/bash

set -e

export GOMAXPROCS=2

STORAGE_ROOT="/tmp/metrics_bench"
VALUES_PER_TIMESERIES=43200 # 15 days, with 2 samples per minute

. set_env.sh

for num_ts in 5 20 100 1000 10000 50000; do
  # Cleanup previous runs.
  rm -rf ${STORAGE_ROOT}*

  # Populate database.
  tools/populator/populator -storage.root="${STORAGE_ROOT}" -deleteStorage=false -numTimeseries="${num_ts}" -numValuesPerTimeseries=43200

  for group_size in 200 500 1000 5000 10000; do
    compacted_root="${STORAGE_ROOT}_group_${group_size}"
    cp -r "${STORAGE_ROOT}" "${compacted_root}"
    # Compact samples in database.
    tools/compactor/compactor -storage.root="${compacted_root}" -compact.ageInclusiveness=1m -compact.groupSize=5000

    # Run queries.
    echo "=== num_ts: ${num_ts} group_size: ${group_size}"
    tools/querier/querier -storage.root="${compacted_root}" -alsologtostderr -range=48h -expression="metric_0" -expectedElements=1 -resolution=1m
    rm -rf "${compacted_root}"
  done
done
