#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Quick commands (copy-paste on Ed, under project1/):
#
# 1) Official command from spec.md (full data, 2 reducers):
#    python3 proj1.py -r hadoop latency2401.csv -o output \
#      --jobconf myjob.settings.tau=50 \
#      --jobconf mapreduce.job.reduces=2
#
# 2) Run this one-click test script (recommended):
#    bash run_tests_ed.sh
#
# 3) Custom tau/reducers:
#    bash run_tests_ed.sh 50 2
#
# 4) Include full dataset test:
#    bash run_tests_ed.sh 50 2 --full
#
# Params:
#   $1 = tau (default: 50)
#   $2 = reducers for multi-reducer test (default: 2)
#   $3 = --full to run full dataset Hadoop test (optional)
# ============================================================

TAU="${1:-50}"
REDUCERS="${2:-2}"
RUN_FULL="${3:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "==> Running tests in: ${SCRIPT_DIR}"
echo "==> tau=${TAU}, reducers=${REDUCERS}"

echo
echo "==> [1/4] Local runner sanity test on sample.csv (tau=5)"
python3 proj1.py -r local sample.csv --jobconf myjob.settings.tau=5 > my_sample_out.txt
echo "Sample output saved to: my_sample_out.txt"
echo "--- sample output preview ---"
head -n 20 my_sample_out.txt || true
echo "-----------------------------"

echo
echo "==> [2/4] Hadoop test on latency2401_10pct.csv (1 reducer)"
hdfs dfs -rm -r -f output_10pct_test >/dev/null 2>&1 || true
python3 proj1.py -r hadoop latency2401_10pct.csv -o output_10pct_test \
  --jobconf myjob.settings.tau="${TAU}" \
  --jobconf mapreduce.job.reduces=1

hdfs dfs -cat output_10pct_test/part-* | sort > my_10pct_tau${TAU}.txt
sort output_10pct_tau50 > ref_10pct_tau50.txt

if [[ "${TAU}" == "50" ]]; then
  echo "==> Comparing against reference output_10pct_tau50"
  if diff -u ref_10pct_tau50.txt my_10pct_tau${TAU}.txt > diff_10pct_tau${TAU}.txt; then
    echo "PASS: 10% dataset matches reference for tau=50."
  else
    echo "WARN: Differences found. See diff_10pct_tau${TAU}.txt"
  fi
else
  echo "NOTE: tau != 50, skipped strict diff with output_10pct_tau50."
fi

echo
echo "==> [3/4] Hadoop multi-reducer smoke test (reducers=${REDUCERS})"
hdfs dfs -rm -r -f output_10pct_r${REDUCERS} >/dev/null 2>&1 || true
python3 proj1.py -r hadoop latency2401_10pct.csv -o output_10pct_r${REDUCERS} \
  --jobconf myjob.settings.tau="${TAU}" \
  --jobconf mapreduce.job.reduces="${REDUCERS}"

echo "Reducer outputs:"
hdfs dfs -ls output_10pct_r${REDUCERS}

echo
echo "==> [4/4] Optional full dataset test"
if [[ "${RUN_FULL}" == "--full" ]]; then
  hdfs dfs -rm -r -f output_full_test >/dev/null 2>&1 || true
  python3 proj1.py -r hadoop latency2401.csv -o output_full_test \
    --jobconf myjob.settings.tau="${TAU}" \
    --jobconf mapreduce.job.reduces="${REDUCERS}"
  echo "Full dataset test completed: output_full_test"
else
  echo "Skipped full dataset test. Use --full as 3rd argument to enable."
fi

echo
echo "==> Done."
echo "Generated files:"
echo "  - my_sample_out.txt"
echo "  - my_10pct_tau${TAU}.txt"
echo "  - ref_10pct_tau50.txt"
echo "  - diff_10pct_tau${TAU}.txt (if diff was run)"
