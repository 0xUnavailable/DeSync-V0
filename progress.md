
#  DESYNC Progress Report

This file tracks **development milestones**, **design choices**, and **next steps** for the DESYNC execution layer.

---

##  Implemented Features

### 1. Deterministic Ordering

* Transactions ordered by `(BlockHeight, IndexInBlock, TxID)`.
* Guarantees all nodes process the same ordered sequence.

### 2. Parallel Batch Execution

* Transactions split into batches.
* Batches executed by workers in parallel.
* Results merged deterministically into global state.

### 3. Intra-Node Execution (CPU Cores)

* Extended execution model to further subdivide batches across **CPU cores**.
* Implemented two approaches:

  * **Static partitioning** → even split across cores.
  * **Work stealing** → idle cores grab leftover work.
* Achieved higher throughput with multi-core scaling.

### 4. Staging + Deterministic Merge

* Workers stage writes in-memory before DB commit.
* Final merge applies keys in **sorted order** → avoids Go map nondeterminism.

### 5. Optimizations

* `sync.Pool` used for `KVResult` slices.
* Reduced allocations and GC pressure.
* Batched DB writes to reduce syscall overhead.

### 6. Merkle Root Computation

* Current: iterate DB → compute root from scratch.
* Verifies final state after every block.
* Pending upgrade to **incremental authenticated structure**.

### 7. Benchmarking

* Benchmarks for **constrained vs unconstrained vs intra-node execution**.
* CSV output + matplotlib charts with error bars.
* Reached \~**1.4M TPS** (synthetic benchmark, 1M tx in <700ms).

---

##  Work in Progress

1. **Deterministic Merge Order**  --- DONE

   * Hardening: all DB commits sorted by key.

2. **Deterministic Parallelism**

   * Next step: **static key-sharding** (`hash(key) % N`).
   * Removes conflicts → ensures deterministic results.

3. **Incremental Merkle Tree** ---- (IN PROGRESS SPARKLE MERKLE TREE + LEVELS DB)

   * Replace `IterateAll` with IAVL or Sparse Merkle.
   * Enables O(log n) updates + efficient proofs.

4. **Stable Receipts/Proof Encoding**

   * Switch from Gob → Protobuf.
   * Deterministic, cross-language serialization.

5. **State Sync & Recovery**

   * Add **snapshots + replay**.
   * Supports fast bootstrap and crash recovery.

6. **ABCI / Tendermint Integration**

   * DeliverTx, CheckTx, Commit.
   * Consensus-level validation of deterministic roots.

---

##  What We Rejected

* **Parallel DB Writers (RocksDB-style)**

  * DB not in hot path → all writes happen once per block.
  * Parallelizing DB layer adds complexity with no gain.
  * LevelDB (or Pebble later) is sufficient.

---
