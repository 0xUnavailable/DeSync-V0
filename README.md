# DeSync

DeSync is a prototype blockchain-inspired city-building game in which blockchain concepts are abstracted into a game world. Each player controls a plot of land (their in-game wallet) and engages in building activities to increase the value of that plot. The system uses a leader/worker model for transaction collection, parallel execution and finality.

This README explains the game concept, the runtime architecture, the transaction model, how to run the prototype locally, and developer notes.

---

## Concept / Game overview

- **Plots as wallets**  
  Each plot of land is a player’s wallet. A plot is identified by a public key address. Everything that constitutes a plot (stones, buildings, resources, etc.) is an asset attached to that plot and can be transferred between players.

- **Gameplay mechanics**
  - Players improve their plots by performing "building" activities (e.g., Grow) that increase resources or value on the plot.
  - At the end of every allotted period, an imperial tax is collected on each plot.
  - Failing to meet the tax quota may attract penalties. This mechanic encourages participation and ongoing engagement.
  - Transfers of assets between plots are equivalent to transfers of value between wallets.

- **Motivation**  
  The goal is to provide a game experience that uses blockchain ideas for ownership and transfers, while hiding blockchain complexity from players. Players interact with plots and assets rather than raw accounts and gas mechanics.

---

## System architecture

DeSync is implemented as a leader + workers system:

1. **Loadgen / Clients** submit transactions (tx) to the network by publishing to the leader’s transaction topic.
2. **Leader**:
   - Receives and queues transactions.
   - Periodically forms batches (size and interval configured by flags).
   - Takes an authoritative snapshot of the current state and publishes the batch plus snapshot to a worker topic.
3. **Workers**:
   - Subscribe to batches from the leader.
   - Execute the batch deterministically against the provided snapshot in parallel (sharding is supported).
   - Produce a `TxResult` per transaction (success/fail, write set, events), sign the results and publish a signed result message.
4. **Leader** collects worker results, applies `WriteSet` entries from successful transactions into its authoritative state, and creates/commits blocks for finality.

This design decouples execution (workers) from finality (leader) and lets you scale execution throughput independently.

---

## Transactions (actions)

The prototype supports these actions:

- **AllocateLand**  
  Create a new plot and assign an owner. Payload: `plotID`, `owner`.

- **Grow**  
  Increase a resource or building stat on a plot. Payload: `plotID`, `amount`.

- **Transfer**  
  Transfer value (or assets) between plots (players). Payload: `from`, `to`, `amount`.

- **PayTax**  
  Pay the plot’s tax to the owner or treasury. Payload: `plotID`, `amount`.

Each transaction is signed by the sender’s private key and workers validate signatures before execution.

---

## Core files (high-level)

- `cmd/node` — node CLI (leader or worker).
- `consensus/leader.go` — leader logic: tx ingestion, batching, snapshot creation, result handling, block commit.
- `consensus/worker.go` — worker logic: batch execution, signing results, logging.
- `execution/sandbox.go` — sandboxed executor that applies transactions against a snapshot and returns `TxResult.WriteSet`.
- `network` — pubsub topics, message structs (TxMessage, BatchMessage, SignedResult, etc.).
- `cmd/loadgen` — load generator and seeding logic (faucet, plot allocation, spam traffic).
- `cmd/inspect` — inspect committed block files.

---

## Running locally (Makefile targets)

A `Makefile` is provided to run common flows. Before running workers, start the leader, copy the leader peer ID from its logs and paste it into the `LEADER_PEERID` variable inside the provided `Makefile`.

### Start the leader
```bash
make leader
````

### Start workers (each target runs a single worker; run them in separate terminals)

```bash
make worker1
make worker2
make worker3
make worker4
# Additional worker targets can be added to the Makefile if needed
```

### Start load generator

```bash
make loadgen
```

Loadgen performs:

1. Seeding phase (faucet funds + AllocateLand per player),
2. Waits for seed confirmations (or times out),
3. Starts configured high-rate spam traffic.

### Inspect committed blocks

```bash
make inspect
```

### Clean up generated keys and blocks

```bash
make clean
```

---

## CLI flags (node)

### General (leader & worker)

* `--role` — `leader` or `worker`.
* `--id` — node id string (e.g. `leader1`, `worker1`).
* `--port` — TCP port for libp2p listen.
* `--keyfile` — path to persist the node libp2p key.
* `--connect` — multiaddr of bootstrap node (used by workers to connect to leader). Example:

  ```
  --connect=/ip4/127.0.0.1/tcp/9000/p2p/<leader-peer-id>
  ```

### Leader-specific

* `--batchsize` — maximum tx per batch (e.g. 1000).
* `--batchinterval` — flush interval if batch not full (e.g. 2s).
* `--blockinterval` — commit blocks every interval (if txs exist).
* `--queuemon` — queue monitor interval for logging.

### Worker-specific

* Requires `--id`, `--port`, `--keyfile`, `--connect`.

---

## Load generator flags (`cmd/loadgen`)

* `--connect` — leader multiaddr (same as worker connect).
* `--workers` — number of simulated players (affects seeding and transfers).
* `--rate` — transactions per second per simulated player.
* `--txcount` — total transactions to send (0 = unlimited).
* `--seedtimeout` — how long to wait for seed confirmations.

---

## Seeding and faucet behavior

* The load generator seeds the world by:

  * Funding player accounts from a special `faucet` account.
  * Allocating a set of plot IDs to each player and recording these locally.
* To keep stress tests simple, a faucet behavior is implemented so loadgen funding succeeds; this can be:

  * a one-time large balance initialization on first faucet use, or
  * an infinite faucet configuration (executor-level change) if desired.
* Leader applies worker result `WriteSet`s into authoritative state. Leader snapshots are included in batches so workers execute against the same view.

---

## Observability and debugging

* Worker logs show per-batch:

  * total txs, success count, fail count, elapsed time.
  * breakdown of failure reasons (e.g., `insufficient balance`, `plot not allocated`).
* Leader logs show:

  * published batch IDs and snapshot size.
  * applied writes when results are received.
* If you see large numbers of `plot not allocated`, check:

  1. That leader is including snapshots when publishing batches.
  2. That seeding (AllocateLand) was confirmed and applied before spam started.
* If you see `insufficient balance`, either increase seed funding, reduce tax/transfer amounts, or top-up via faucet.

---

## Development notes

* State persistence is currently in-memory for quick iteration (MemStore). Replace with LevelDB or SMT for persistence and proofs.
* Leader snapshot mechanism ensures deterministic worker execution for a given batch. Consider optimizing snapshot size (deltas) for throughput.
* The prototype intentionally separates execution and finality to explore designs where execution scales independently of consensus.
* Transaction signatures are verified by workers. Ensure clients sign correctly before publishing to the network.

---

## Extensibility ideas

* Persistent storage (LevelDB) + inclusion of compact Merkle commitments (SMT) for proofs.
* Sharded execution across multiple workers per batch.
* More nuanced economic mechanics (production cycles, maintenance, repair).
* Player client that hides signatures and keys behind UX while still mapping to plot addresses.
* On-chain bridges to real tokens or off-chain marketplaces for plot assets.

---

## Testing

* Use the `cmd/loadgen` seeding and limited `txcount` modes to test deterministic outcomes.
* Unit tests for the SMT, memstore, and sandbox executor exist under `state/` and `execution/`.

---

## Contributing

* Clone the repo, run `go mod tidy`.
* New features should include tests where applicable.
* Send pull requests with a clear description of the design and rationale.

---
