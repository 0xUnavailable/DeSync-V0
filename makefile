# Makefile for running tests, benchmarks, and race detection

.PHONY: test testv race bench bench-long all clean \
        test-parallel test-deterministic test-vrf test-multibatch \
        bench-unconstrained bench-constrained \
        bench-unconstrained-20k bench-unconstrained-200k bench-unconstrained-1m \
        bench-constrained-20k bench-constrained-200k bench-constrained-1m \
        list

# Run all tests (normal mode)
test:
	go test ./...

# Run all tests with verbose logs
testv:
	go test -v ./...

# Run all tests with race detector enabled
race:
	go test -race -v ./...

# Run benchmarks only
bench:
	go test -bench=. ./...

# Run benchmarks with longer time per test for stable numbers
bench-long:
	go test -bench=. -benchtime=30s ./...

# Run both tests and benchmarks
all:
	go test -v -bench=. ./...

# Clean up test caches & temp dirs
clean:
	go clean -testcache
	rm -rf bench_db_* pf_db_* deterministic_db_* *.csv
	rm -rf smt_test_db_*
	rm -rf deterministic_*
	rm -rf bench_*
	rm -rf pf_txs.json

# -----------------------
# Individual test targets
# -----------------------

test-parallel:
	go test -v -run ^TestMeasureParallelFraction$$ ./...

test-deterministic:
	go test -v -run ^TestDeterministicExecution$$ ./...

test-vrf:
	go test -v -run ^TestVRFProveVerify$$ ./...

test-multibatch:
	go test -v -run ^TestMultiBatchReceipts$$ ./...

# -----------------------
# Individual benchmark targets
# -----------------------

bench-unconstrained:
	go test -bench ^BenchmarkScalingUnconstrained$$ -benchmem ./...

bench-constrained:
	go test -bench ^BenchmarkScalingConstrained$$ -benchmem ./...

bench-unconstrained-20k:
	go test -bench ^BenchmarkScalingUnconstrained/20000tx_unconstrained$$ -benchmem ./...

bench-unconstrained-200k:
	go test -bench ^BenchmarkScalingUnconstrained/200000tx_unconstrained$$ -benchmem ./...

bench-unconstrained-1m:
	go test -bench ^BenchmarkScalingUnconstrained/1000000tx_unconstrained$$ -benchmem ./...

bench-constrained-20k:
	go test -bench ^BenchmarkScalingConstrained/20000tx_constrained$$ -benchmem ./...

bench-constrained-200k:
	go test -bench ^BenchmarkScalingConstrained/200000tx_constrained$$ -benchmem ./...

bench-constrained-1m:
	go test -bench ^BenchmarkScalingConstrained/1000000tx_constrained$$ -benchmem ./...

# -----------------------
# Utility
# -----------------------

list:
	@echo ""
	@echo "Available make targets:"
	@echo "  test                → Run all tests"
	@echo "  testv               → Run all tests with verbose output"
	@echo "  race                → Run all tests with race detector"
	@echo "  bench               → Run all benchmarks"
	@echo "  bench-long          → Run benchmarks with 30s per test"
	@echo "  all                 → Run tests + benchmarks"
	@echo "  clean               → Cleanup temporary DBs and CSVs"
	@echo ""
	@echo "  test-parallel       → Run TestMeasureParallelFraction"
	@echo "  test-deterministic  → Run TestDeterministicExecution"
	@echo "  test-vrf            → Run TestVRFProveVerify"
	@echo "  test-multibatch     → Run TestMultiBatchReceipts"
	@echo ""
	@echo "  bench-unconstrained       → Run all BenchmarkScalingUnconstrained cases (full parallelism, single commit per block)"
	@echo "  bench-constrained         → Run all BenchmarkScalingConstrained cases (worker fluctuation, sequential per-batch commits)"
	@echo "  bench-unconstrained-20k   → Run 20,000 tx unconstrained benchmark"
	@echo "  bench-unconstrained-200k  → Run 200,000 tx unconstrained benchmark"
	@echo "  bench-unconstrained-1m    → Run 1,000,000 tx unconstrained benchmark"
	@echo "  bench-constrained-20k     → Run 20,000 tx constrained benchmark"
	@echo "  bench-constrained-200k    → Run 200,000 tx constrained benchmark"
	@echo "  bench-constrained-1m      → Run 1,000,000 tx constrained benchmark"
	@echo ""
