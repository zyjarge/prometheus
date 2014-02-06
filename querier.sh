#!/bin/bash

set -e

export GOMAXPROCS=4

STORAGE_ROOT="/srv/metrics_bench"

function query {
  storage_root="${1}"
  expression="${2}"
  tools/querier/querier -storage.root="${storage_root}" -alsologtostderr -range=48h -expression="${expression}" -expectedElements=1 -resolution=1m -numWarmupQueries=5 -numQueries=10 2>&1
}

function bench {
  expr_name="${1}"
  expr_query="${2}"

  for num_ts in 20 100 1000 2500 5000 10000 15000; do
    for group_size in 200 500 1000 5000 10000; do
      echo "=== QUERYING num_ts: ${num_ts} group_size: ${group_size} ${expr_name}"
      compacted_root="${STORAGE_ROOT}/compacted_${num_ts}_group_${group_size}"
      out="$(query "${compacted_root}" "${expr_query}")"
      echo "$out"
      disk_time=$(echo "${out}" | grep "disk data extraction time" | cut -d" " -f7)
      total_view_time=$(echo "${out}" | grep "Total view building time" | cut -d" " -f6)
      total_eval_time=$(echo "${out}" | grep "Total eval time" | cut -d" " -f5)
      echo "BENCHRESULT ${num_ts} ${group_size} ${expr_name} - - - ${total_eval_time} ${total_view_time} ${disk_time}"
    done
  done
}

for i in 1 10; do
  expression="$(for n in $(seq 1 $i); do echo -n "metric_${n} + "; done)"
  expression="${expression%???}"
  bench "${i}" "${expression}"
done
