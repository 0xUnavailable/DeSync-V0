# DESYNC Protocol

DESYNC is a next-generation execution-layer protocol designed for **deterministic transaction ordering, parallelized batch execution, and verifiable state transitions**.  

Unlike traditional blockchain systems where ordering and execution are tightly coupled (causing bottlenecks), DESYNC **separates ordering from execution**, allowing multiple workers to process batches of transactions deterministically while preserving a globally consistent state root.

---

## Why DESYNC?

Blockchains today face a tradeoff:
- **Throughput vs. Security**: Higher parallelism often leads to non-determinism or execution conflicts.  
- **Ordering Fairness**: MEV and frontrunning distort fair ordering.  
- **Scalability**: Monolithic execution bottlenecks performance.  

DESYNC solves this by:
- Using **deterministic transaction ordering** (based on block height + index).  
- **Splitting execution into batches** processed by multiple worker nodes.  
- **Verifiable receipts and proofs** (Merkle + VRF) to confirm correctness.  
- **Leader election with VRF** to avoid bias and manipulation.  

---

## Architecture Overview

### High-Level Flow

```mermaid
flowchart TD
    A[Transactions from Users] --> B[Deterministic Ordering]
    B --> C[Batch Creation]
    C --> D[Assigned to Workers]
    D --> E[Execution with State Machine]
    E --> F[BatchResult + Proofs]
    F --> G[Receipt Verification]
    G --> H[New State Root]
````

* **Ordering**: Transactions are sorted by `(BlockHeight, IndexInBlock, TxID)`.
* **Batching**: Ordered txs are split into fixed-size batches.
* **Workers**: Each batch is assigned to a worker node.
* **Execution**: Workers apply transactions deterministically.
* **Proofs**: Workers generate inclusion proofs & receipts.
* **Verification**: Other nodes verify receipts against state roots.

---

### Node Election (VRF)

```mermaid
sequenceDiagram
    participant Seed as Epoch Seed
    participant Node1
    participant Node2
    participant Node3

    Seed->>Node1: VRF Prove(seed)
    Seed->>Node2: VRF Prove(seed)
    Seed->>Node3: VRF Prove(seed)

    Node1->>Coordinator: Beta1, Proof1
    Node2->>Coordinator: Beta2, Proof2
    Node3->>Coordinator: Beta3, Proof3

    Coordinator->>Coordinator: Verify all proofs
    Coordinator->>Coordinator: Select lowest Beta = Leader
```

* Each node runs **VRF Prove(seed)**.
* All outputs are verified publicly.
* The node with the **lowest VRF output** is elected leader.

---

## Key Components

* **Transactions** → Basic structure with `TxID`, `Nonce`, `GasPrice`, etc.
* **Batches** → Deterministically grouped sets of txs.
* **Execution State** → Maintained as key/value store with Merkle proofs.
* **Receipts** → Contain `PostStateRoot` + signatures for verification.
* **Evidence** → Mechanism for slashing misbehaving nodes.
* **Persistence** → Backed by LevelDB for durability.

---

## Example Output

```bash
DESYNC prototype (updated). Demo run...
restored epoch: 1
Created 3 batches
Batch 1 -> post_root 7e1b4c...
Receipt verified for batch 1
Batch 2 -> post_root 9a3d12...
Receipt verified for batch 2
Batch 3 -> post_root 6b42f8...
Receipt verified for batch 3
```

---

##  Roadmap

* [x] Deterministic transaction ordering
* [x] Merkle state root computation
* [x] VRF-based leader election (EC-VRF)
* [x] LevelDB persistence
* [ ] ABCI interface (`CheckTx`, `DeliverTx`, `Commit`)
* [ ] Tendermint integration
* [ ] Evidence + slashing rules
* [ ] Networking (libp2p/gRPC)
* [ ] Stress testing at 10k+ TPS
