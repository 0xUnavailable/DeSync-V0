# DESYNC Makefile Commands

This document explains the available `make` commands defined in the Makefile.

---

## 🔹 Test Commands

```bash
# Run all tests (default)
make test
# Run all tests with verbose output
make testv
# Run tests with the Go race detector enabled
make race

# Run all benchmarks (default settings)
make bench

# Run benchmarks with longer execution time (10s per test)
make bench-long

# Run both tests and benchmarks together
make all

# Clean up build artifacts, cached test results, and temporary benchmark DBs
make clean


This will remove:

Go test cache

Temporary databases (bench_db_*, pf_db_*, deterministic_db_*, smt_test_db_*)

Benchmark data (*.csv, bench_*, pf_txs.json)


Notes

bench and bench-long generate CSVs (e.g., scaling_unconstrained.csv) which can be visualized with:

python3 plot_scaling_comparison.py