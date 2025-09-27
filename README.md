# DeSync

DeSync is a blockchain-envisioned **city-building game** where blockchain concepts are abstracted into gameplay.  
Instead of wallets and tokens, each player controls **a plot of land**, represented by a public key address.  

Players compete to make their land more valuable by engaging in building activities. Periodically, an **imperial tax** is collected.  
Failure to meet the quota may incur penalties, encouraging participation and resource optimization.

---

## Architecture

DeSync operates with a **Leader / Worker** model:

- **Leader**  
  - Collects transactions submitted via HTTP or pubsub.  
  - Batches them into fixed-size groups.  
  - Sends batches to workers for execution.  
  - Collects results and commits blocks.  
  - At month-end (configurable), enforces taxes across all plots.

- **Workers**  
  - Subscribe to batches.  
  - Execute game transactions deterministically in sandboxed state.  
  - Return write-sets and receipts to the leader.

- **Clients**  
  - Players submit transactions (e.g. AllocateLand, Grow, Transfer, PayTax).  
  - Load generators (`loadgen` or `loadgen_http`) simulate large transaction volumes.  
  - API endpoints allow exploration of state and blocks.

---

## Node CLI Flags

### General
- `--role` — `leader` or `worker`.  
- `--id` — unique identifier (e.g., `leader1`, `worker1`).  
- `--port` — TCP port to listen on. Defaults: 9000 for leader, 9001+ for workers.  
- `--keyfile` — path to persist libp2p identity.  
- `--connect` — multiaddress to connect to leader (`/ip4/127.0.0.1/tcp/9000/p2p/<peer-id>`).  

### Leader-Specific
- `--batchsize` — max transactions per batch.  
- `--batchinterval` — flush interval for batches.  
- `--blockinterval` — interval for committing blocks.  
- `--queuemon` — log frequency for tx queue length.  
- `--apiport` — port for HTTP API (default `8080`).  
- `--monthblocks` — number of blocks in one "month" (for tax enforcement).  

### Worker-Specific
- Must specify `--id`, `--port`, `--keyfile`, and `--connect`.

---

## Game Actions

- **AllocateLand** — assign ownership of a new plot.  
- **Grow** — increase resources of a plot.  
- **Transfer** — transfer assets between players.  
- **PayTax** — voluntarily pay tax quota.  
- **Month-End Enforcement** — at the end of each month (N blocks), leader collects tax automatically and applies penalties.

---

## API Endpoints

### Transaction submission
`POST /tx`  
Submit a transaction.

Example:
```bash
curl -X POST http://localhost:8080/tx -H 'Content-Type: application/json' -d '{
  "Sender":"player0",
  "Nonce":1,
  "Action":"AllocateLand",
  "Payload":{"plotID":"plot-0-0","owner":"player0"}
}'
