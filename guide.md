# Comprehensive Guide to Setting Up and Using the DESYNC Distributed Task Processing System

This Markdown guide provides a thorough, step-by-step explanation of how to set up, configure, and use the DESYNC package—a lightweight, Go-based distributed system for task batching, leader election, and parallel execution. Adapted for "web2" applications (e.g., task queues, microservices, or data processing pipelines), DESYNC operates in time-bounded epochs, supports dynamic node joining, and handles basic fault tolerance via heartbeats and elections.

The system models "transactions" as tasks (arbitrary `[]byte` data), which a leader collects, batches, and assigns to workers. Workers process tasks using a custom `StateMachine` interface. Results are finalized by the leader. This guide includes a practical example: a distributed sum calculator using JSON tasks.

**Key Features:**
- Epoch-based cycles (processing + election phases).
- Random leader election (lowest random hash wins).
- Dynamic joining with majority approval.
- Persistence for crash recovery.
- RPC-based communication.

**Assumptions:**
- Trusted nodes (no security features).
- Local state management (extend for shared DB if needed).
- Tested on Go 1.20+.

## Prerequisites
Before starting, ensure you have:
- **Go**: Version 1.20 or later. Install from [golang.org/dl](https://golang.org/dl/).
- **OS**: Linux, macOS, or Windows (use WSL for multi-node testing on one machine).
- **Hardware/Setup**: 2-4 terminals or machines for nodes (local ports for simulation).
- **Tools**: Text editor (e.g., VS Code with Go extension), terminal.
- **Dependencies**: None; uses Go standard library only.

No internet or external packages required beyond what's in the code.

## Step 1: Project Setup
1. Create a new directory for your project:
   ```
   mkdir desync-project
   cd desync-project
   ```

2. Initialize a Go module:
   ```
   go mod init github.com/yourusername/desync-project
   ```

3. Copy the DESYNC code into a file named `desync/desync.go` (create a `desync` subdir for organization).

4. Run `go mod tidy` to resolve any implicit dependencies (though none are added).

5. Create a `main.go` file in the root for launching nodes (example in Step 4).

6. Create a `metadata` directory for persistence files:
   ```
   mkdir metadata
   ```

## Step 2: Implementing a Custom StateMachine
The `StateMachine` interface defines task execution logic. Implement `Apply(cmd []byte) ([]byte, error)` to process each task.

### Basic Example: Echo Processor
For testing, an echo that returns the input:
```go
// In desync/desync.go or a separate file
type EchoStateMachine struct{}

func (esm *EchoStateMachine) Apply(cmd []byte) ([]byte, error) {
    return cmd, nil
}
```

### Practical Example: JSON Sum Calculator
For the example app: Process JSON tasks like `{"numbers": [1, 2, 3]}` to compute sums.
```go
// Add to desync/desync.go or main.go
import (
    "encoding/json"
    "fmt"
)

type SumStateMachine struct{}

func (ssm *SumStateMachine) Apply(cmd []byte) ([]byte, error) {
    var task struct {
        Numbers []int `json:"numbers"`
    }
    if err := json.Unmarshal(cmd, &task); err != nil {
        return nil, err
    }
    sum := 0
    for _, n := range task.Numbers {
        sum += n
    }
    result := map[string]int{"sum": sum}
    return json.Marshal(result)
}
```

Extend this for real apps (e.g., integrate a database for persistent state).

## Step 3: Configuring the Cluster
Define initial nodes and genesis settings.

### Example Configuration (3-Node Local Cluster)
Add this to `main.go` or a config file:
```go
var clusterConfig = []desync.ClusterMember{
    {Id: 1, Address: "127.0.0.1:8001"},
    {Id: 2, Address: "127.0.0.1:8002"},
    {Id: 3, Address: "127.0.0.1:8003"},
}

var genesisConfig = &desync.GenesisConfig{
    ClusterMembers:   clusterConfig,
    GenesisTimestamp: uint64(time.Now().UnixMicro() + 5000000), // Delay start by 5 seconds
    EpochDuration:    "180s",
    BootstrapNodes:   []string{"127.0.0.1:8001"}, // For dynamic joins
}
```

- **Node IDs**: Unique uint64 values.
- **Addresses**: IP:port; use public IPs for remote setups.
- **GenesisTimestamp**: Unix microsecond when the cluster can start (set future for controlled launch).
- **BootstrapNodes**: Existing nodes for new joiners to query.

Customize constants in `desync.go` (e.g., shorten `EpochDuration` to 30s for testing).

## Step 4: Starting the Nodes
Each node needs its own process. Use terminals to simulate.

### Example main.go for Node Launch
```go
package main

import (
    "fmt"
    "os"
    "time"

    "./desync" // Adjust if in subdir
)

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Usage: go run main.go <clusterIndex>")
        return
    }
    clusterIndex := os.Args[1] // e.g., "0" for node 1

    // From Step 3
    // clusterConfig and genesisConfig defined here...

    sm := &SumStateMachine{} // Your StateMachine
    metadataDir := "./metadata"

    index := 0 // Parse from args in real use
    // For demo, hardcode or parse
    server := desync.NewServer(clusterConfig, genesisConfig, sm, metadataDir, index)
    server.Debug = true
    server.Start()

    select {} // Run forever
}
```

- Launch nodes:
  - Terminal 1: `go run main.go 0` (node 1, index 0)
  - Terminal 2: `go run main.go 1` (node 2)
  - Terminal 3: `go run main.go 2` (node 3)

- What Happens:
  - Nodes restore from persistence (or initialize).
  - If genesis conditions met, they elect a leader after stagger delay.
  - Logs show: "[DEBUG] [Id:1, Epoch:1] Participating in election", role assignments.

For remote: Run on separate machines, update addresses.

## Step 5: Submitting and Processing Tasks (Practical Example)
Once running, submit tasks to the leader.

### Example Client Code (Add to main.go or Separate File)
```go
// In a separate client.go or integrate
import (
    "encoding/json"
    "fmt"
    "time"
)

// Assume server is your desync.Server instance (or RPC to leader)
func submitTasks(server *desync.Server) {
    tasks := [][]byte{
        []byte(`{"numbers": [1, 2, 3]}`), // Sum = 6
        []byte(`{"numbers": [4, 5, 6]}`), // Sum = 15
    }

    for _, task := range tasks {
        txID := server.AcceptTransaction(task)
        if txID != nil {
            fmt.Printf("Submitted task %x\n", txID)
        } else {
            fmt.Println("Not leader - find leader via query")
        }
        time.Sleep(500 * time.Millisecond) // Simulate arrival
    }
}
```

- Run: Call `submitTasks(server)` on the leader node (identify via logs).
- Process:
  - Leader pools tasks.
  - Batches form (e.g., after 1s or 1000 tasks).
  - Assigned to workers (e.g., node 2/3 process sums).
  - Results collected; state updated (if changes populated).
  - Bundled (logs "Bundled to L1"; customize to print sums).

### Extending for Results
Modify `bundleToL1` to output results:
```go
func (s *Server) bundleToL1(results []BatchResult) {
    for _, res := range results {
        for _, exec := range res.ExecutionResults {
            if exec.Success {
                var output map[string]int
                json.Unmarshal(exec.ResultData, &output)
                fmt.Printf("Task %x sum: %d\n", exec.TxID, output["sum"])
            }
        }
    }
    s.debug("Bundled results")
}
```

Expected Output (in logs/console):
- "Submitted task <hash>"
- Worker execution (debug if enabled).
- "Bundled to L1" with sums: e.g., "Task <hash> sum: 6"

## Step 6: Dynamic Node Joining
Add a node 4:
- Update main.go for index 0, new config: `clusterConfig = []ClusterMember{{Id: 4, Address: "127.0.0.1:8004"}}`
- Run: `go run main.go`
- It queries bootstrap (:8001), joins, syncs cluster.
- Logs: "Synced state from leader"

## Step 7: Monitoring, Debugging, and Troubleshooting
- **Enable Debug**: `server.Debug = true` for detailed logs (epochs, batches, roles).
- **Common Errors**:
  - "Error dialing": Check ports open, addresses correct.
  - No batches: Ensure tasks submitted to leader.
  - Election fails: Too few nodes (needs >=2 for leader+backup).
- **Recovery**: Kill/restart a node; it restores from metadata.
- **Customization**:
  - Shorten epochs for testing.
  - Add worker heartbeats to update `LastSeen`.
  - Integrate DB in StateMachine for persistent data.

## Step 8: Production Considerations
- **Deployment**: Docker/Kubernetes for nodes.
- **Scaling**: 10+ nodes; monitor RPC latency.
- **Security**: Add auth/TLS to RPC.
- **Testing**: Unit tests for StateMachine; integration with mock clusters.

This guide covers a full setup with a sum calculator example. For questions, extend the code or consult Go docs. Happy distributing!