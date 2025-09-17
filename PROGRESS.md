# DESYNC Development Progress

This file tracks the ongoing implementation log for the DESYNC protocol.

---

## ✅ Completed Milestones

### Core Protocol
- [x] **Core types** (`types.go`) → Transactions, Batches, Receipts, etc.
- [x] **State database integration** (`statedb_smt.go`)  
  - Unified LevelDB storage.  
  - Integrated **Sparse Merkle Tree (SMT)** for verifiable state transitions.  
  - Incremental updates on every `PutState`.  
- [x] **Sparse Merkle Tree (SMT)** (`smt.go`)  
  - Minimal prototype, depth = 256 (SHA-256).  
  - Proof generation + verification.  
  - Zero-hash optimization for empty subtrees.  
- [x] **Execution pipeline** (`execution.go`)  
  - Batch execution.  
  - Deterministic merge ordering.  
- [x] **Worker orchestration** (`workers.go`)  
  - Parallel execution across workers.  
  - Intra-node parallel execution across CPU cores.  
- [x] **Utilities** (`utils.go`)  
  - Hashing, sorting, transaction ordering.  
- [x] **Demo runner** (`main.go`)  
  - Demonstrates batch creation, worker assignment, and state root commits.  

### Testing & Benchmarks
- [x] **Test suite** (`desync_test.go`)  
  - Determinism checks across workers/cores.  
  - Scaling benchmarks (unconstrained vs constrained vs intra-node).  
  - Parallel fraction measurement (Amdahl’s Law).  
  - VRF leader election test (via `ecvrf`).  
- [x] **Benchmark results CSVs**  
  - `scaling_unconstrained.csv`  
  - `scaling_constrained.csv`  
  - `scaling_intra.csv`  
- [x] **Visualization script** (`plot_scaling_comparison.py`)  
  - Bar chart comparison with error bars.  
  - Aggregated TPS vs block size plots.

### Developer Workflow
- [x] **Makefile**
  - `make test` → run all tests.  
  - `make testv` → verbose tests.  
  - `make race` → tests with race detector.  
  - `make bench` → run all benchmarks.  
  - `make bench-long` → stable benchmarks (30s per test).  
  - `make all` → tests + benchmarks.  
  - `make clean` → cleanup temporary DBs and CSVs.  

#### 🔍 Run individual tests
  - `make test-parallel` → run only **TestMeasureParallelFraction**.  
  - `make test-deterministic` → run only **TestDeterministicExecution**.  
  - `make test-vrf` → run only **TestVRFProveVerify**.  
  - `make test-multibatch` → run only **TestMultiBatchReceipts**.  

#### ⚡ Run individual benchmarks
  - `make bench-unconstrained` → run all **BenchmarkScalingUnconstrained** cases.  
  - `make bench-constrained` → run all **BenchmarkScalingConstrained** cases.  

#### 🎯 Run sub-benchmarks by dataset size
  - `make bench-unconstrained-20k` → 20,000 tx unconstrained case.  
  - `make bench-unconstrained-200k` → 200,000 tx unconstrained case.  
  - `make bench-unconstrained-1m` → 1,000,000 tx unconstrained case.  
  - `make bench-constrained-20k` → 20,000 tx constrained case.  
  - `make bench-constrained-200k` → 200,000 tx constrained case.  
  - `make bench-constrained-1m` → 1,000,000 tx constrained case.  

#### 📋 Utility
  - `make list` → print all available commands and their descriptions.  


- [x] **Documentation**  
  - Updated `README.md` for usage + benchmarks.  
  - Added `MAKE_COMMANDS.md` with developer workflow.  

---

## 📊 Current Status

- Core SMT-based execution pipeline **working**.  
- Deterministic tests confirm identical state roots across all worker/core configs.  
- Benchmarks reach **1M+ TPS** in local tests.  
- Visualization pipeline for scaling results established.  

---

## 🚧 Next Steps (Planned)

- [ ] **Optimize SMT**  
  - Improve proof compression.  
  - Explore batch updates for high-throughput workloads.  
- [ ] **Persistent receipts**  
  - Store per-tx receipts with Merkle proofs for light clients.  
- [ ] **Distributed multi-node testing**  
  - Validate consensus safety across multiple physical nodes.  
- [ ] **Advanced leader election**  
  - VRF integration into block proposal pipeline.  
- [ ] **State pruning & compaction**  
  - Keep DB size manageable during long runs.  

---

## ⚠️ Gotchas & Known Issues

- **LevelDB “MANIFEST-000000 missing” error during benchmarks**  
  - Cause: `resetTestDBBench` deletes DB dirs while benchmarks run concurrently.  
  - Impact: Only happens in benchmarking mode with parallel `go test`.  
  - Fix: Ensure DB reset/cleanup is isolated per test run.  
  - Production Impact: **Not an issue** in real deployments (DB not deleted mid-run).  

- **SMT DeleteState behavior**  
  - Deletion is represented by writing an empty value instead of structural removal.  
  - Root remains consistent, but proofs may include “empty” hashes.  
  - Future Work: Implement true removal or pruning.  

- **Benchmark scaling**  
  - Throughput gains flatten beyond ~20 cores due to DB writes and state merge bottlenecks.  
  - Next steps: batch DB writes, async I/O, or sharded state.  

---

