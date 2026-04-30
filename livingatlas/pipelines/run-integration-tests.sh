#!/bin/bash

TEST_DIR="src/test/java"
FAILED_TESTS=()
PASSED_TESTS=()
MAX_PARALLEL=${MAX_PARALLEL:-2}  # default 4, override with MAX_PARALLEL=8 ./run-integration-tests.sh

# Temp dir to track results
RESULTS_DIR=$(mktemp -d)

download_shapefiles() {
  if find /tmp/pipelines-shp/ -name "*.shp" -type f 2>/dev/null | grep -q .; then
    echo "Shapefiles already exist, skipping download."
  else
    echo "Shapefiles not found, downloading..."
    mvn download:wget@install-shapefiles
    echo "Shapefiles downloaded."
  fi
}

run_test() {
  local TEST=$1
  mvn failsafe:integration-test failsafe:verify \
    -Dit.test="$TEST" \
    -DforkCount=1 \
    -DreuseForks=false \
    -Dsurefire.skip=true
}

retry_test() {
  local TEST=$1
  echo "Retrying in a fresh JVM: $TEST"
  sleep 5
  run_test "$TEST"
}

run_test_with_result() {
  local TEST=$1
  local RESULT_FILE="$RESULTS_DIR/$TEST"

  echo "Starting: $TEST"

  if run_test "$TEST"; then
    echo "PASSED: $TEST"
    echo "PASSED" > "$RESULT_FILE"
  else
    echo "FAILED: $TEST — retrying in fresh JVM..."
    if retry_test "$TEST"; then
      echo "PASSED on retry: $TEST"
      echo "PASSED_ON_RETRY" > "$RESULT_FILE"
    else
      echo "FAILED on retry: $TEST"
      echo "FAILED" > "$RESULT_FILE"
    fi
  fi
}

# Step 1 - Download shapefiles
download_shapefiles

# Step 2 - Find all integration test classes (ending in IT.java)
IT_TESTS=$(find "$TEST_DIR" -name "*IT.java" -type f | \
  sed 's|.*/||' | \
  sed 's|\.java||')

if [ -z "$IT_TESTS" ]; then
  echo "No integration tests found in $TEST_DIR"
  exit 1
fi

echo "Found the following integration tests:"
echo "$IT_TESTS"
echo "======================================="
echo "Running with MAX_PARALLEL=$MAX_PARALLEL threads"
echo "======================================="

# Step 3 - Run tests in parallel with a concurrency limit
ACTIVE_JOBS=0
declare -a JOB_PIDS=()

for TEST in $IT_TESTS; do
  # Run test in background
  run_test_with_result "$TEST" &
  JOB_PIDS+=($!)
  ACTIVE_JOBS=$((ACTIVE_JOBS + 1))

  # If we hit the parallel limit, wait for one job to finish
  if [ "$ACTIVE_JOBS" -ge "$MAX_PARALLEL" ]; then
    wait "${JOB_PIDS[0]}"
    JOB_PIDS=("${JOB_PIDS[@]:1}")  # remove first element
    ACTIVE_JOBS=$((ACTIVE_JOBS - 1))
  fi
done

# Wait for all remaining jobs to finish
echo "Waiting for remaining tests to finish..."
for PID in "${JOB_PIDS[@]}"; do
  wait "$PID"
done

# Step 4 - Collect results
echo ""
echo "======================================="
echo "SUMMARY"
echo "======================================="

for RESULT_FILE in "$RESULTS_DIR"/*; do
  TEST=$(basename "$RESULT_FILE")
  RESULT=$(cat "$RESULT_FILE")

  case $RESULT in
    PASSED)
      PASSED_TESTS+=("$TEST")
      echo "  ✓ $TEST"
      ;;
    PASSED_ON_RETRY)
      PASSED_TESTS+=("$TEST (passed on retry)")
      echo "  ✓ $TEST (passed on retry)"
      ;;
    FAILED)
      FAILED_TESTS+=("$TEST")
      echo "  ✗ $TEST"
      ;;
  esac
done

# Cleanup temp dir
rm -rf "$RESULTS_DIR"

echo ""
echo "Passed:  ${#PASSED_TESTS[@]}"
echo "Failed:  ${#FAILED_TESTS[@]}"

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
  echo ""
  echo "Some tests failed after retry!"
  exit 1
else
  echo ""
  echo "All tests passed!"
fi