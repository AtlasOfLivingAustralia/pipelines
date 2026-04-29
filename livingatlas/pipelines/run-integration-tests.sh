#!/bin/bash

TEST_DIR="src/test/java"
FAILED_TESTS=()
PASSED_TESTS=()

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

# Download shapefiles once before running any tests
download_shapefiles

# Find all integration test classes (ending in IT.java)
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

# Run each test individually
for TEST in $IT_TESTS; do
  echo ""
  echo "Running: $TEST"
  echo "---------------------------------------"

  if run_test "$TEST"; then
    echo "PASSED: $TEST"
    PASSED_TESTS+=("$TEST")
  else
    echo "FAILED: $TEST — retrying in fresh JVM..."

    if retry_test "$TEST"; then
      echo "PASSED on retry: $TEST"
      PASSED_TESTS+=("$TEST (passed on retry)")
    else
      echo "FAILED on retry: $TEST"
      FAILED_TESTS+=("$TEST")
    fi
  fi
done

# Summary
echo ""
echo "======================================="
echo "SUMMARY"
echo "======================================="
echo "Passed (${#PASSED_TESTS[@]}):"
for t in "${PASSED_TESTS[@]}"; do echo "  ✓ $t"; done

echo "Failed (${#FAILED_TESTS[@]}):"
for t in "${FAILED_TESTS[@]}"; do echo "  ✗ $t"; done

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
  echo ""
  echo "Some tests failed after retry!"
  exit 1
else
  echo ""
  echo "All tests passed!"
fi