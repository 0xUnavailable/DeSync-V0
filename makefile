# Makefile for running tests, benchmarks, and race detection

.PHONY: test bench race all clean

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
	go test -bench=. -benchtime=10s ./...

# Run both tests and benchmarks
all:
	go test -v -bench=. ./...

# Clean up test caches & temp dirs
clean:
	go clean -testcache
	rm -rf bench_db_* pf_db_* deterministic_db_* *.csv
	rm -rf smt_test_db
