# DESYNC Protocol - Developer Implementation Specification

## Core Data Structures

### Node State Structure
```
NodeState {
    // Identity & Network
    node_id: string (32-byte hex identifier derived from public key)
    address: network_address (IP:PORT or p2p multiaddr)
    public_key: 256-bit public key (Ed25519 or ECDSA secp256k1)
    private_key: corresponding 256-bit private key (secure storage required)
    
    // Cluster Role & Epoch
    role: enum { "worker", "leader", "backup_leader" }
    current_epoch: integer (monotonically increasing)
    epoch_start_time: timestamp_milliseconds
    
    // Leadership Information
    leader_id: string | null
    backup_leader_id: string | null
    leader_last_seen: timestamp_milliseconds
    backup_last_seen: timestamp_milliseconds
    
    // Consensus State
    stake: integer (wei or smallest unit)
    state_root: 32-byte hash (current execution state merkle root)
    execution_state: key_value_map (state_key -> state_value)
    previous_state_root: 32-byte hash (for rollup continuity)
    
    // Election Data
    voted_hash_commit: 32-byte hash | null
    vrf_hash: 32-byte hash | null
    vrf_proof: byte_array | null
    revealed_hash: 32-byte hash | null
    
    // Transaction Processing
    tx_pool: map (tx_id -> Transaction)
    batch_sequence: integer (monotonically increasing)
    worker_batches: map (worker_id -> Batch)
    worker_results: map (worker_id -> BatchResult)
    current_batch: Batch | null (for workers)
    
    // Membership Management
    pending_join_requests: map (node_id -> JoinRequest)
    last_heartbeat: map (node_id -> timestamp_milliseconds)
    
    // Persistence Flags
    persistent_data_version: integer (for migration handling)
    last_checkpoint_epoch: integer
}
```

### Configuration Structures
```
GenesisConfig {
    genesis_timestamp: timestamp_milliseconds (network launch time)
    minimum_stake: integer (minimum wei to participate)
    total_stake: integer (sum of all member stakes, updated dynamically)
    bootstrap_nodes: array[network_address] (initial discovery endpoints)
    
    // Performance Parameters
    max_batch_size: integer (transactions per batch)
    batch_timeout: milliseconds (maximum batch creation delay)
    epoch_duration: milliseconds (normal epoch length)
    election_commit_phase: milliseconds (40000 default)
    election_reveal_phase: milliseconds (20000 default)
    
    // Fault Tolerance
    heartbeat_interval: milliseconds (100 default)
    heartbeat_timeout: milliseconds (500 default)
    leader_failure_delay: milliseconds (1000 emergency election delay)
    
    // Economic Parameters
    leader_fee_percentage: decimal (0.0 to 1.0)
    challenger_reward_percentage: decimal (0.1 default)
    slashing_penalties: map (violation_type -> penalty_amount)
}

ClusterMember {
    id: string (node identifier)
    address: network_address
    public_key: 256-bit key
    stake: integer
    last_seen: timestamp_milliseconds
    reputation_score: decimal (0.0 to 1.0, affects batch assignments)
    join_epoch: integer (when member joined)
    status: enum { "active", "offline", "slashed", "departing" }
}
```

### Transaction & Execution Structures
```
Transaction {
    tx_id: 32-byte hash (SHA256 of transaction data)
    tx_data: byte_array (raw transaction bytes)
    arrival_timestamp: timestamp_microseconds (for deterministic ordering)
    gas_price: integer (for priority ordering within same timestamp)
    sender_address: 20-byte address
    nonce: integer (replay protection)
    signature: cryptographic_signature
}

Batch {
    batch_id: 32-byte hash (SHA256 of batch contents)
    sequence: integer (leader-assigned sequential number)
    transactions: array[Transaction] (ordered list)
    worker_id: string (assigned executor)
    created_timestamp: timestamp_milliseconds
    expected_completion: timestamp_milliseconds (for timeout detection)
}

TransactionResult {
    tx_id: 32-byte hash
    success: boolean
    result_data: byte_array (execution output)
    gas_used: integer
    state_changes: array[StateChange]
    error_message: string | null
}

StateChange {
    key: byte_array (state storage key)
    old_value: byte_array | null
    new_value: byte_array | null
    operation: enum { "create", "update", "delete" }
}

BatchResult {
    batch_id: 32-byte hash
    worker_id: string
    execution_results: array[TransactionResult]
    new_state_root: 32-byte hash
    execution_proof: MerkleProof
    worker_signature: cryptographic_signature
    completion_timestamp: timestamp_milliseconds
}
```

### Messaging Structures
```
JoinRequest {
    node_id: string
    address: network_address
    public_key: 256-bit key
    stake: integer
    signature: cryptographic_signature (signs concatenated fields)
    timestamp: timestamp_milliseconds
    version: string (protocol version compatibility)
}

VRFCommit {
    sender_id: string
    epoch: integer
    hash_commit: 32-byte hash (SHA256(vrf_hash || node_id || epoch))
    timestamp: timestamp_milliseconds
}

VRFReveal {
    sender_id: string
    epoch: integer
    revealed_hash: 32-byte hash (original VRF output)
    vrf_proof: byte_array (cryptographic proof of correctness)
}

Heartbeat {
    sender_id: string
    epoch: integer
    role: enum { "leader", "backup_leader" }
    current_state_root: 32-byte hash
    batch_sequence: integer (for sync checking)
    timestamp: timestamp_milliseconds
}
```

## Function Specifications

### Node Initialization Functions

**InitializeNode() → void**
```
Purpose: Bootstrap node from cold start or crash recovery
Prerequisites: Configuration files loaded, network connectivity established
Process:
    1. Load persistent_state from disk (create empty if missing)
    2. Load genesis_config and cluster_config from configuration
    3. Initialize cryptographic keys (generate if new node)
    4. If empty state: set defaults (epoch=0, role=worker, stake=0)
    5. Determine startup path: CheckGenesisConditions()
    6. Execute appropriate startup: HandleGenesisStartup() OR JoinExistingCluster()
    7. Start background services: StartEventLoop(), StartEpochTimer()
Outputs: Node ready for network participation
Error Handling: Log errors, retry with exponential backoff, fail safely to worker mode
```

**CheckGenesisConditions() → boolean**
```
Purpose: Determine if this is genesis cluster initialization
Inputs: genesis_config, current_state, current_time
Logic:
    return genesis_config.exists AND 
           current_state.current_epoch == 0 AND 
           current_time >= genesis_config.genesis_timestamp
```

**HandleGenesisStartup() → void**
```
Process:
    1. Set current_epoch = 1
    2. Calculate stagger_delay = hash(node_id) % 100 milliseconds
    3. Sleep(stagger_delay) to prevent election conflicts
    4. Call ParticipateInElection()
```

### Cluster Joining Functions

**JoinExistingCluster() → void**
```
Purpose: Integrate with running cluster
Process:
    1. Call QueryClusterForLeader() with bootstrap_nodes
    2. If no leader found: WaitForElectionPhase(), return
    3. Update state.leader_id, state.backup_leader_id, state.current_epoch
    4. Create and sign JoinRequest message
    5. Send synchronous message to leader with 10-second timeout
    6. If approved: RequestStateSyncFromLeader()
    7. If rejected: WaitForNextEpoch(), retry
Error Handling: Exponential backoff, maximum 5 retry attempts
```

**HandleNodeJoinRequest(request: JoinRequest) → void**
```
Purpose: Process join request (leader only)
Preconditions: state.role == "leader"
Process:
    1. If NOT at_epoch_boundary(): queue request, respond with "queued"
    2. Validate: VerifyJoinSignature(request) AND request.stake >= minimum_stake
    3. If invalid: respond with rejection reason
    4. Store in pending_join_requests
    5. BroadcastMessage("node_join_request", request) to cluster
    6. StartApprovalTimer(request.node_id, 10000ms)
```

**ProcessJoinApprovals(node_id: string) → void**
```
Purpose: Aggregate cluster approval votes
Process:
    1. Calculate total_approval_stake from collected approvals
    2. If total_approval_stake >= (2/3 * genesis_config.total_stake):
        - Create ClusterMember object from pending request
        - Update genesis_config.total_stake += new_member.stake
        - Sign cluster update message
        - BroadcastMessage("cluster_member_update", update_message)
        - SendMessage(node_id, "node_join_response", approved=true)
        - Remove from pending_join_requests
```

### VRF Election Functions

**ParticipateInElection() → void**
```
Purpose: Engage in leader election process
Preconditions: state.stake >= genesis_config.minimum_stake
Process:
    1. Calculate epoch_seed = ComputeEpochSeed(current_epoch)
    2. If IsCommitPhase(): SubmitVRFCommit(epoch_seed)
    3. If IsRevealPhase() AND voted_hash_commit exists: SubmitVRFReveal()
```

**ComputeEpochSeed(epoch: integer) → 32-byte hash**
```
Purpose: Generate deterministic randomness seed for VRF
Logic:
    if epoch == 1 AND genesis_config.exists:
        return SHA256(genesis_config.genesis_timestamp || epoch)
    else:
        return SHA256(previous_epoch_final_state_root || epoch || previous_leader_vrf)
```

**SubmitVRFCommit(epoch_seed: byte_array) → void**
```
Process:
    1. Generate: vrf_hash, vrf_proof = VRF_Generate(epoch_seed, private_key)
    2. Create commit = SHA256(vrf_hash || node_id || current_epoch)
    3. Store: state.vrf_hash, state.vrf_proof, state.voted_hash_commit
    4. BroadcastMessage("vrf_commit", commit_message)
```

**HandleVRFReveal(message: VRFReveal) → void**
```
Purpose: Process and validate reveal messages
Process:
    1. Verify commitment: expected_commit = SHA256(revealed_hash || sender_id || epoch)
    2. Check: pending_commits[sender_id] == expected_commit
    3. Verify VRF proof: VRF_Verify(revealed_hash, proof, sender_pubkey, epoch_seed)
    4. If valid: store in valid_reveals[sender_id] = {hash, stake}
```

**FinalizeElection() → void**
```
Purpose: Determine election winner and transition epoch
Process:
    1. Calculate participating_stake = sum(reveal.stake for valid_reveals)
    2. If participating_stake < (2/3 * total_stake): ExtendElectionPhase(), return
    3. Create weighted_reveals[] array:
        - For each valid reveal: weighted_hash = vrf_hash / stake
        - Sort by weighted_hash ascending
    4. Set new_leader = lowest weighted_hash, new_backup = second lowest
    5. If elected as leader/backup: StartHeartbeatTimer(100ms)
    6. If elected as leader: ProcessPendingJoins()
    7. TransitionToNewEpoch(new_leader, new_backup)
```

### Transaction Processing Functions

**HandleTransaction(tx_data: byte_array) → void**
```
Purpose: Process incoming transaction (leader only)
Preconditions: Validate transaction format and signature
Process:
    1. If state.role != "leader": ForwardToLeader(tx_data), return
    2. Generate tx_id = SHA256(tx_data)
    3. Create Transaction object with arrival_timestamp = current_time_microseconds()
    4. Store: state.tx_pool[tx_id] = transaction
    5. If ShouldCreateBatch(): CreateAndDistributeBatches()
```

**ShouldCreateBatch() → boolean**
```
Logic:
    return tx_pool.size >= max_batch_size OR 
           (current_time - last_batch_time) >= batch_timeout
```

**CreateAndDistributeBatches() → void**
```
Purpose: Create sequential batches for worker execution
Process:
    1. Create ordered_txs = OrderTransactionsDeterministically()
    2. Get active_workers = GetActiveWorkers() (exclude offline/slashed)
    3. Calculate batch_size = ceil(ordered_txs.length / active_workers.length)
    4. For each worker (index i):
        - Extract slice: transactions[i*batch_size : (i+1)*batch_size]
        - Create Batch with sequence = state.batch_sequence++
        - Store: state.worker_batches[worker_id] = batch
        - SendMessage(worker_id, "batch_assignment", batch)
    5. Clear: state.tx_pool
```

**OrderTransactionsDeterministically() → array[Transaction]**
```
Purpose: Ensure consistent transaction ordering across all nodes
Logic:
    return sort(tx_pool.values, comparator: (a, b) => {
        if a.arrival_timestamp != b.arrival_timestamp:
            return a.arrival_timestamp - b.arrival_timestamp
        else:
            return string_compare(a.tx_id, b.tx_id)
    })
```

### Worker Execution Functions

**HandleBatchAssignment(batch: Batch) → void**
```
Purpose: Execute assigned transaction batch (worker only)
Preconditions: state.role == "worker"
Process:
    1. Set state.current_batch = batch
    2. result = ExecuteBatchWithDependencies(batch)
    3. SendMessage(state.leader_id, "batch_result", result)
```

**ExecuteBatchWithDependencies(batch: Batch) → BatchResult**
```
Purpose: Execute transactions with cross-worker state resolution
Process:
    1. Initialize: execution_results = [], current_state = copy(execution_state), state_changes = []
    2. For each transaction in batch.transactions:
        - required_keys = AnalyzeTransactionDependencies(transaction)
        - missing_state = RequestMissingState(required_keys, current_state)
        - Merge missing_state into current_state
        - result = ExecuteSingleTransaction(transaction, current_state)
        - Add result to execution_results
        - Apply result.state_changes to current_state and state_changes
    3. new_state_root = ComputeStateRoot(state_changes)
    4. execution_proof = CreateMerkleProof(state_changes, new_state_root)
    5. Sign result and return BatchResult
```

**RequestMissingState(required_keys: array[byte_array], current_state: map) → map**
```
Purpose: Resolve cross-worker state dependencies
Process:
    1. missing_keys = filter(required_keys, key => key not in current_state)
    2. If missing_keys.empty: return empty_map
    3. target_workers = FindWorkersWithState(missing_keys)
    4. requested_state = empty_map
    5. For each worker_id in target_workers:
        - Create StateRequest with missing_keys and current batch_id
        - response = SendMessageSync(worker_id, "state_dependency_request", request, 5000ms)
        - Merge response.state_data into requested_state
    6. Return requested_state
Error Handling: Timeout → retry once, then proceed with available state
```

### Batch Result Aggregation Functions

**HandleBatchResult(result: BatchResult) → void**
```
Purpose: Aggregate worker execution results (leader only)
Preconditions: state.role == "leader"
Process:
    1. Verify: VerifyWorkerSignature(result, worker_public_key)
    2. If invalid signature: SlashStake(result.worker_id, "invalid_signature"), return
    3. Store: state.worker_results[result.worker_id] = result
    4. If AllBatchesCompleted(): FinalizeBatchExecution()
```

**AllBatchesCompleted() → boolean**
```
Logic:
    return worker_results.keys.length == worker_batches.keys.length AND
           all(worker_id in worker_batches.keys => worker_id in worker_results.keys)
```

**FinalizeBatchExecution() → void**
```
Purpose: Apply results sequentially and resolve conflicts
Process:
    1. ordered_results = sort(worker_results.values, key: batch.sequence)
    2. final_state_root = state.state_root, final_results = []
    3. For each result in ordered_results:
        - If VerifyBatchExecution(result, final_state_root):
            - Add result to final_results
            - Update final_state_root = result.new_state_root
        - Else:
            - SlashStake(result.worker_id, "invalid_execution")
            - corrected_result = ReExecuteBatch(result.batch, final_state_root)
            - Add corrected_result to final_results
            - Update final_state_root = corrected_result.new_state_root
    4. Update: state.state_root = final_state_root
    5. BundleToL1(final_results)
    6. Clear: state.worker_batches, state.worker_results
```

### Heartbeat & Failure Detection Functions

**StartHeartbeatTimer(interval: milliseconds) → void**
```
Purpose: Begin heartbeat transmission (leader/backup only)
Process:
    1. Create recurring timer with specified interval
    2. On each tick: SendHeartbeat()
```

**SendHeartbeat() → void**
```
Purpose: Broadcast liveness signal
Preconditions: state.role in ["leader", "backup_leader"]
Process:
    1. Create heartbeat_message with current state information
    2. BroadcastMessage("heartbeat", heartbeat_message)
```

**HandleHeartbeat(message: Heartbeat) → void**
```
Purpose: Update liveness tracking
Process:
    1. If message.sender_id == state.leader_id: state.leader_last_seen = message.timestamp
    2. If message.sender_id == state.backup_leader_id: state.backup_last_seen = message.timestamp
    3. Update: state.last_heartbeat[message.sender_id] = message.timestamp
```

**CheckForFailures() → void**
```
Purpose: Detect and respond to node failures (called every 100ms)
Process:
    1. current_time = GetCurrentTimestamp()
    2. leader_timeout = (current_time - state.leader_last_seen) > heartbeat_timeout
    3. backup_timeout = (current_time - state.backup_last_seen) > heartbeat_timeout
    4. If leader_timeout AND NOT backup_timeout: PromoteBackupToLeader()
    5. If leader_timeout AND backup_timeout: TriggerEmergencyElection()
```

**TriggerEmergencyElection() → void**
```
Purpose: Initiate emergency leader selection
Process:
    1. Sleep(genesis_config.leader_failure_delay) for network stabilization
    2. Increment: state.current_epoch++
    3. Reset: state.role = "worker"
    4. Call: ParticipateInElection()
```

### State Management Functions

**LoadPersistentState() → NodeState**
```
Purpose: Restore node state from disk storage
Process:
    1. Attempt to read state file from designated path
    2. If file exists: deserialize and validate version compatibility
    3. If file missing or corrupt: return empty NodeState with defaults
    4. Apply any necessary migrations based on version differences
Error Handling: Log corruption issues, backup corrupt files, use defaults
```

**SavePersistentState(state: NodeState) → void**
```
Purpose: Persist critical node state to disk
Process:
    1. Serialize state to binary or JSON format
    2. Write to temporary file first (atomic operation)
    3. Rename temporary file to replace current state file
    4. Verify write succeeded, log any failures
Frequency: After each epoch transition, major state changes
```

**RequestStateSyncFromLeader() → void**
```
Purpose: Synchronize state after joining or recovery
Process:
    1. Create sync_request with last_known_epoch and state_root
    2. response = SendMessageSync(leader_id, "state_sync_request", request, 30000ms)
    3. If incremental sync: ApplyStateDelta(response.state_delta)
    4. If full sync required: RebuildFullState(response.full_state)
    5. Verify final state consistency with expected root
```

### L1 Integration Functions

**BundleToL1(finalized_results: array[BatchResult]) → void**
```
Purpose: Create and submit rollup bundle to L1 contract
Process:
    1. Extract transaction_data from successful executions
    2. Aggregate state_updates from all batch results
    3. Calculate total_fees and gas_used across all transactions
    4. Create rollup_bundle with state proofs and signatures
    5. Submit bundle to L1 contract with retry logic
    6. Monitor submission confirmation and handle rejections
```

**HandleL1Confirmation(bundle_hash: string, confirmation: object) → void**
```
Purpose: Process L1 submission results
Process:
    1. If confirmation.success: FinalizeStateTransition(bundle_hash)
    2. If confirmation.failed: InvestigateBundleRejection(bundle_hash, confirmation.reason)
```

## Implementation Logic Patterns

### Error Handling Patterns

**Network Communication Errors:**
```
Pattern: Retry with exponential backoff
Implementation:
    - Initial retry delay: 100ms
    - Maximum retry delay: 30000ms (30 seconds)
    - Maximum retry attempts: 5
    - Double delay after each failure
    - Log all retry attempts for debugging
```

**State Inconsistency Errors:**
```
Pattern: Graceful degradation and recovery
Implementation:
    - Detect through state root mismatches
    - Attempt incremental sync first (faster)
    - Fall back to full state rebuild if needed
    - Enter safe mode if sync repeatedly fails
    - Alert monitoring systems for manual intervention
```

**Byzantine Behavior Detection:**
```
Pattern: Immediate isolation and punishment
Implementation:
    - Verify all signatures immediately upon receipt
    - Cross-validate execution results against known state
    - Slash stake immediately for proven fraud
    - Remove from active participant lists
    - Broadcast fraud evidence to cluster
```

### Concurrency Patterns

**Message Processing:**
```
Pattern: Event-driven with message queues
Implementation:
    - Single-threaded message processing to avoid race conditions
    - Prioritized message queues (heartbeats > elections > transactions)
    - Timeout handling for all synchronous operations
    - Batch processing for high-volume message types
```

**State Access:**
```
Pattern: Copy-on-write with atomic updates
Implementation:
    - Create state snapshots for transaction execution
    - Apply changes atomically after validation
    - Use locks only for critical state transitions
    - Implement optimistic concurrency where possible
```

### Performance Optimization Patterns

**Batch Processing:**
```
Pattern: Adaptive batch sizing with feedback loops
Implementation:
    - Monitor average batch processing time per worker
    - Adjust batch size based on target latency (1 second)
    - Consider transaction complexity in sizing decisions
    - Implement worker capacity scoring for load balancing
```

**State Storage:**
```
Pattern: Hierarchical state management with pruning
Implementation:
    - Hot state: Recent transactions in memory
    - Warm state: Recent epochs on fast storage
    - Cold state: Historical epochs on archive storage
    - Automatic pruning based on age and access patterns
```

### Security Implementation Patterns

**Cryptographic Operations:**
```
Pattern: Defense in depth with multiple verification layers
Implementation:
    - Verify message signatures before any processing
    - Use secure random number generation for all nonces
    - Implement constant-time comparison functions
    - Regular key rotation for long-running deployments
```

**Input Validation:**
```
Pattern: Strict validation with early rejection
Implementation:
    - Validate all message formats before deserialization
    - Check all numeric ranges and bounds
    - Verify timestamp reasonableness (not too old/future)
    - Reject malformed or suspicious messages immediately
```

This specification provides concrete implementation guidance while remaining language-agnostic. Each function includes clear inputs, outputs, error handling requirements, and the specific logic steps needed for correct implementation.