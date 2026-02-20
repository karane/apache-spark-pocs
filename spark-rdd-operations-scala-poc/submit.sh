#!/usr/bin/env bash
set -e

JOBS=(
  Ex01_CreatingRddsJob
  Ex02_BasicTransformationsJob
  Ex03_PairRddOperationsJob
  Ex04_WordCountJob
  Ex05_SetOperationsJob
  Ex06_ActionsJob
  Ex07_PartitioningJob
  Ex08_AdvancedPairRddJob
  Ex09_LogAnalysisJob
  Ex10_TransactionAnalyticsJob
)

usage() {
  echo "Usage: $0 <JobName|all>"
  echo ""
  echo "Available jobs:"
  for job in "${JOBS[@]}"; do
    echo "  $job"
  done
  echo "  all   — run every job in sequence"
  exit 1
}

submit_job() {
  local class="org.karane.$1"
  echo ""
  echo ">>> Submitting $1..."
  docker run --rm \
    --network spark-rdd-operations-scala-poc_default \
    -v "$(pwd)/app/target/scala-2.12:/app" \
    -v "$(pwd)/app/data:/data" \
    apache/spark:3.5.0 \
    /opt/spark/bin/spark-submit \
      --class "$class" \
      --master spark://spark-master:7077 \
      /app/spark-rdd-operations-scala_2.12-1.0.jar
}

[[ $# -lt 1 ]] && usage

if [[ "$1" == "all" ]]; then
  for job in "${JOBS[@]}"; do
    submit_job "$job"
  done
else
  # Validate
  valid=false
  for job in "${JOBS[@]}"; do
    [[ "$1" == "$job" ]] && valid=true && break
  done
  if [[ "$valid" == "false" ]]; then
    echo "Unknown job: $1"
    usage
  fi
  submit_job "$1"
fi
