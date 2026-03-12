#!/usr/bin/env bash
set -euo pipefail

# Tunable workload profiles for scale validation.
# Override by exporting env vars before running this script.
CORRECTNESS_THREADS="${CORRECTNESS_THREADS:-12}"
CORRECTNESS_MESSAGES="${CORRECTNESS_MESSAGES:-500}"
CORRECTNESS_PAYLOAD_BYTES="${CORRECTNESS_PAYLOAD_BYTES:-256}"

PERF_THREADS="${PERF_THREADS:-16}"
PERF_MESSAGES="${PERF_MESSAGES:-800}"
PERF_PAYLOAD_BYTES="${PERF_PAYLOAD_BYTES:-512}"
PERF_MIN_THROUGHPUT="${PERF_MIN_THROUGHPUT:-0}"
PERF_MAX_P95_MS="${PERF_MAX_P95_MS:-0}"

echo "Starting proxy scale correctness + performance suite..."
echo "Correctness profile: threads=${CORRECTNESS_THREADS}, messages/thread=${CORRECTNESS_MESSAGES}, payload=${CORRECTNESS_PAYLOAD_BYTES}B"
echo "Performance profile: threads=${PERF_THREADS}, messages/thread=${PERF_MESSAGES}, payload=${PERF_PAYLOAD_BYTES}B"

mvn test -Dtest=ProxyScaleTestSuite \
  -Dscale.correctness.threads="${CORRECTNESS_THREADS}" \
  -Dscale.correctness.messages="${CORRECTNESS_MESSAGES}" \
  -Dscale.correctness.payloadBytes="${CORRECTNESS_PAYLOAD_BYTES}" \
  -Dscale.perf.threads="${PERF_THREADS}" \
  -Dscale.perf.messages="${PERF_MESSAGES}" \
  -Dscale.perf.payloadBytes="${PERF_PAYLOAD_BYTES}" \
  -Dscale.perf.minThroughput="${PERF_MIN_THROUGHPUT}" \
  -Dscale.perf.maxP95Ms="${PERF_MAX_P95_MS}"

echo "Scale suite completed successfully."
