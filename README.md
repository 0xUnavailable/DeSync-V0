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
- `--batchsize` — max transactions per batch (default `1000`).  
- `--batchinterval` — flush interval for batches (default `2s`).  
- `--blockinterval` — interval for committing blocks (default `10s`).  
- `--queuemon` — log frequency for tx queue length (default `5s`).  
- `--apiport` — port for HTTP API (default `8080`).  
- `--monthblocks` — number of blocks in one "month" (for tax enforcement) (default `10`).  
- `--staticdir` — directory to serve static files (e.g., `ui/` for `explorer.html`).

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
````

---

## Explorer UI

The built-in **block explorer** can be served directly by the leader node.

Run the leader with the `--staticdir` flag pointing to the UI folder:

```bash
make leader STATICDIR=ui
```

Then open in your browser:

```
http://localhost:8080/explorer.html
```

Features:

* View **status** (peer ID, role, readiness, block count).
* Inspect the **latest block**.
* Browse **all transactions** (with search + pagination).
* View **tax events** from the latest month summary.

---

## Makefile Targets

Run `make list` to see available targets. Key commands include:

- `make leader` — Start the leader node (API + Explorer UI on `:8080`).
- `make worker1` — Start worker1 on port `9001`.
- `make worker2` — Start worker2 on port `9002`.
- `make worker3` — Start worker3 on port `9003`.
- `make workers` — Start all three workers.
- `make loadgen` — Run libp2p-based load generator (100k transactions).
- `make loadgen_http` — Run HTTP-based load generator (100k transactions).
- `make inspect` — Inspect committed blocks in `./blocks`.
- `make demo` — Launch leader, three workers, HTTP loadgen, and serve Explorer UI.
- `make get-leader-id` — Get the leader's peer ID from running process.
- `make clean` — Remove blocks, keys, logs, and PID files.

### Running the Demo
The `demo` target starts a full setup:
```bash
make demo
```
This launches the leader in background with logs to `leader.log`, discovers the leader ID, starts three workers, runs the HTTP load generator, and serves the Explorer UI at `http://localhost:8080/explorer.html`.

### Worker Setup
Workers require the leader's peer ID. Either:
- Set `LEADER_ID` manually:
  ```bash
  LEADER_ID=12D3KooW... make worker1
  ```
- Or let the Makefile discover it automatically via the leader's API (`http://127.0.0.1:8080/status`).

Use `make get-leader-id` to fetch the leader's peer ID if needed.

---

## Development Notes
- Ensure the leader is running before starting workers or load generators.
- Load generators simulate high transaction volumes (`--txcount=100000`, `--rate=1000`, `--workers=10`).
- Blocks are stored in the `./blocks` directory and can be inspected with `make inspect`.
- Use `make clean` to reset the environment by removing generated files.