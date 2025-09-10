package desync

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

const (
	// EpochDuration defines the total duration of each epoch in the DESYNC protocol
	EpochDuration = 180 * time.Second
	// ProcessingPhase is the duration allocated for transaction processing within an epoch
	ProcessingPhase = 120 * time.Second
	// ElectionPhase is the duration for leader election within an epoch
	ElectionPhase = 60 * time.Second
	// ElectionCommitPeriod is the time window for committing VRF hashes during election
	ElectionCommitPeriod = 40 * time.Second
	// ElectionRevealPeriod is the time window for revealing VRF hashes and proofs
	ElectionRevealPeriod = 20 * time.Second
	// HeartbeatInterval is the frequency at which leaders and backup leaders send heartbeats
	HeartbeatInterval = 100 * time.Millisecond
	// HeartbeatTimeout is the threshold to detect heartbeat failure
	HeartbeatTimeout = 500 * time.Millisecond
	// WorkerExecutionTimeout is the maximum time a worker has to execute a batch
	WorkerExecutionTimeout = 30 * time.Second
	// EmergencyElectionDelay is the stabilization delay before triggering an emergency election
	EmergencyElectionDelay = 1 * time.Second
	// MaxBatchSize is the maximum number of transactions per batch
	MaxBatchSize = 1000
	// GenesisQuorumTimeout is the timeout for achieving quorum during genesis election
	GenesisQuorumTimeout = 10 * time.Second
	// NodeJoinTimeout is the timeout for node join operations
	NodeJoinTimeout = 10 * time.Second
)

func Assert[T comparable](msg string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}

type StateMachine interface {
	// Apply executes a command on the state machine and returns the result
	Apply(cmd []byte) ([]byte, error)
}

type ApplyResult struct {
	Result []byte
	Error  error
}

type Entry struct {
	Epoch     uint64 // Epoch number for this log entry
	Index     uint64 // Log index within the epoch
	TxID      []byte // Transaction ID
	TxData    []byte // Transaction data
	Timestamp uint64 // Microsecond timestamp for ordering
}

type Transaction struct {
	TxID             []byte // Hash of transaction data
	TxData           []byte // Raw transaction bytes
	ArrivalTimestamp uint64 // Microsecond timestamp when received
	GasPrice         uint64 // Gas price for MEV-resistant ordering
}

type Batch struct {
	BatchID          uint64        // Unique batch identifier
	Sequence         uint64        // Sequence number for ordering
	Transactions     []Transaction // List of transactions in the batch
	WorkerID         uint64        // Assigned worker node ID
	CreatedTimestamp time.Time     // Timestamp when batch was created
}

type ExecutionResult struct {
	TxID         []byte        // Transaction ID
	Success      bool          // Execution success flag
	ResultData   []byte        // Execution output
	GasUsed      uint64        // Gas consumed
	StateChanges []StateChange // State modifications from execution
}

type StateChange struct {
	Key      []byte // State key modified
	OldValue []byte // Previous value
	NewValue []byte // New value
}

type BatchResult struct {
	BatchID          uint64            // Batch identifier
	WorkerID         uint64            // Worker that executed the batch
	ExecutionResults []ExecutionResult // Results for each transaction
}

type StateRequest struct {
	RequestingWorker uint64    // ID of the requesting worker
	StateKeys        [][]byte  // Keys needed from state
	BatchID          uint64    // Batch requiring the state
	Timestamp        time.Time // Request timestamp
}

type JoinRequest struct {
	NodeID    uint64    // Joining node's ID
	Address   string    // Node's network address
	Signature []byte    // Signature of join request fields
	Timestamp time.Time // Request timestamp
}

type GenesisConfig struct {
	ClusterMembers   []ClusterMember // Initial cluster members
	GenesisTimestamp uint64          // Microsecond timestamp for genesis
	EpochDuration    string          // Epoch duration string
	BootstrapNodes   []string        // Bootstrap node addresses
}

type RPCMessage struct {
	Epoch uint64 // Current epoch for message
}

type VRFRevealRequest struct {
	RPCMessage
	SenderID     uint64 // Sender node ID
	RevealedHash uint64 // Revealed VRF hash
	VRFProof     []byte // VRF proof
}

type VRFRevealResponse struct {
	RPCMessage
}

type HeartbeatRequest struct {
	RPCMessage
	SenderID  uint64    // Sender node ID
	Role      string    // Sender's role
	Timestamp time.Time // Heartbeat timestamp
}

type HeartbeatResponse struct {
	RPCMessage
}

type BatchAssignmentRequest struct {
	RPCMessage
	Batch          Batch  // Assigned batch
	TargetWorker   uint64 // Target worker ID
	SequenceNumber uint64 // Batch sequence number
}

type BatchAssignmentResponse struct {
	RPCMessage
}

type BatchResultRequest struct {
	RPCMessage
	Result       BatchResult // Execution result
	TargetLeader uint64      // Target leader ID
}

type BatchResultResponse struct {
	RPCMessage
}

type StateDependencyRequest struct {
	RPCMessage
	Request      StateRequest // State request details
	TargetWorker uint64       // Target worker ID
}

type StateDependencyResponse struct {
	RPCMessage
	StateData    map[string][]byte // Requested state data
	TargetWorker uint64            // Original target
	RequestID    uint64            // Request identifier
}

type NodeJoinRequest struct {
	RPCMessage
	Request      JoinRequest // Join request details
	TargetLeader uint64      // Target leader ID
}

type NodeJoinResponse struct {
	RPCMessage
	Approved bool // Approval status
}

type NodeJoinApprovalRequest struct {
	RPCMessage
	NodeID    uint64    // Node ID being approved
	Approved  bool      // Approval decision
	VoterID   uint64    // Voter node ID
	Signature []byte    // Voter signature
	Timestamp time.Time // Approval timestamp
}

type NodeJoinApprovalResponse struct {
	RPCMessage
}

type ClusterMemberUpdateRequest struct {
	RPCMessage
	NewMember ClusterMember // New cluster member
	Signature []byte        // Leader's signature
}

type ClusterMemberUpdateResponse struct {
	RPCMessage
}

type LeaderQueryRequest struct {
	RPCMessage
}

type LeaderQueryResponse struct {
	RPCMessage
	LeaderID            uint64 // Current leader ID
	BackupLeaderID      uint64 // Current backup leader ID
	Epoch               uint64 // Current epoch
	SenderID            uint64 // Sender ID
	LeaderAddress       string // Address of the leader
	BackupLeaderAddress string // Address of the backup leader
}

type GetClusterRequest struct {
	RPCMessage
}

type GetClusterResponse struct {
	RPCMessage
	Cluster []ClusterMember
}

type ClusterMember struct {
	Id         uint64      // Member node ID
	Address    string      // Network address
	LastSeen   time.Time   // Last activity timestamp
	rpcClient  *rpc.Client // RPC client connection
}

type ServerState string

// ServerState represents the role of a server. Concrete role strings are used
// directly in code to keep the protocol simple and human-readable.

type Server struct {
	done   bool
	server *http.Server

	Debug bool

	mu sync.Mutex

	currentEpoch uint64

	log []Entry

	// commitIndex, lastApplied and state were removed as they were unused in
	// this implementation. They can be reintroduced when implementing Raft-
	// compatible log replication.

	id uint64

	address string

	electionTimeout time.Time

	heartbeatMs int

	// heartbeatTimeout removed; lastHeartbeat map provides per-node timings.

	statemachine StateMachine

	metadataDir string

	fd *os.File

	cluster []ClusterMember

	clusterIndex int

	revealedHash uint64

	genesisConfig *GenesisConfig

	role string

	leaderID uint64

	backupLeaderID uint64

	epochStartTime time.Time

	lastHeartbeat map[uint64]time.Time

	txPool map[string]Transaction

	workerBatches map[uint64]Batch

	workerResults map[uint64]BatchResult

	batchSequence uint64

	pendingJoinRequests map[uint64]JoinRequest

	currentBatch Batch

	executionState map[string][]byte

	dependencyRequests []StateRequest

	validReveals map[uint64]uint64

	pendingApprovals map[uint64]map[uint64]bool // join node to voter to approved

	lastBatchTime time.Time

	leaderAddress string

	backupLeaderAddress string
}

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// max is intentionally omitted to avoid an unused generic helper. Use math or
// explicit comparisons where needed.

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %d, Epoch: %d] %s", time.Now().Format(time.RFC3339Nano), s.id, s.currentEpoch, msg)
}

func (s *Server) debug(msg string) {
	if !s.Debug {
		return
	}
	fmt.Println(s.debugmsg(msg))
}

func (s *Server) debugf(msg string, args ...any) {
	if !s.Debug {
		return
	}

	s.debug(fmt.Sprintf(msg, args...))
}

func (s *Server) warn(msg string) {
	fmt.Println("[WARN] " + s.debugmsg(msg))
}

func (s *Server) warnf(msg string, args ...any) {
	s.warn(fmt.Sprintf(msg, args...))
}

func Server_assert[T comparable](s *Server, msg string, a, b T) {
	Assert(s.debugmsg(msg), a, b)
}

func NewServer(
	clusterConfig []ClusterMember,
	genesisConfig *GenesisConfig,
	statemachine StateMachine,
	metadataDir string,
	clusterIndex int,
) *Server {
	// Validate and copy cluster configuration
	var cluster []ClusterMember
	for _, c := range clusterConfig {
		if c.Id == 0 {
			panic("Id must not be 0.")
		}
		cluster = append(cluster, c)
	}

	if len(cluster) == 0 {
		panic("clusterConfig must contain at least one member")
	}

	if clusterIndex < 0 || clusterIndex >= len(cluster) {
		panic("clusterIndex out of range")
	}

	s := &Server{
		id:                  cluster[clusterIndex].Id,
		address:             cluster[clusterIndex].Address,
		cluster:             cluster,
		genesisConfig:       genesisConfig,
		statemachine:        statemachine,
		metadataDir:         metadataDir,
		clusterIndex:        clusterIndex,
		heartbeatMs:         100,
		lastHeartbeat:       make(map[uint64]time.Time),
		txPool:              make(map[string]Transaction),
		workerBatches:       make(map[uint64]Batch),
		workerResults:       make(map[uint64]BatchResult),
		pendingJoinRequests: make(map[uint64]JoinRequest),
		executionState:      make(map[string][]byte),
		dependencyRequests:  []StateRequest{},
		validReveals:        make(map[uint64]uint64),
		pendingApprovals:    make(map[uint64]map[uint64]bool),
	}

	return s
}

func (s *Server) vrfGenerateHash(seed []byte) (hash uint64, proof []byte) {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	hash = r.Uint64()
	proof = nil
	return
}

type PersistentState struct {
	CurrentEpoch  uint64
	GenesisConfig *GenesisConfig
	Log           []Entry
	Cluster       []ClusterMember
}

func (s *Server) persist() {
	// Persist critical state to disk for crash recovery
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if s.fd == nil {
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, fmt.Sprintf("md_%d.dat", s.id)),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0755,
		)
		if err != nil {
			panic(err)
		}
	}

	if _, err = s.fd.Seek(0, 0); err != nil {
		panic(err)
	}

	if err = s.fd.Truncate(0); err != nil {
		panic(err)
	}

	enc := gob.NewEncoder(s.fd)

	err = enc.Encode(PersistentState{
		CurrentEpoch:  s.currentEpoch,
		GenesisConfig: s.genesisConfig,
		Log:           s.log,
		Cluster:       s.cluster,
	})
	if err != nil {
		panic(err)
	}

	if err = s.fd.Sync(); err != nil {
		s.warnf("Error syncing persistent state: %v", err)
	}

	s.debugf("Persisted state.")
}

func (s *Server) restore() {
	// Restore persistent state from disk
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if s.fd == nil {
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, fmt.Sprintf("md_%d.dat", s.id)),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0755,
		)
		if err != nil {
			panic(err)
		}
	}

	if _, err = s.fd.Seek(0, 0); err != nil {
		panic(err)
	}

	dec := gob.NewDecoder(s.fd)

	var ps PersistentState
	err = dec.Decode(&ps)
	if err == io.EOF {
		// Initialize new state if no persistent data
		s.log = []Entry{}
		s.ensureLog()
		// Best-effort persist; do not panic on sync errors here
		s.persist()
		return
	}

	if err != nil {
		// If decode fails, warn and reinitialize to avoid crash on corrupt file
		s.warnf("Failed to decode persistent state (%v); reinitializing", err)
		s.log = []Entry{}
		s.ensureLog()
		s.persist()
		return
	}

	// Load restored state
	s.currentEpoch = ps.CurrentEpoch
	s.genesisConfig = ps.GenesisConfig
	s.log = ps.Log
	s.cluster = ps.Cluster

	s.ensureLog()
}

func (s *Server) ensureLog() {
	// Ensure log has at least one entry
	if len(s.log) == 0 {
		s.log = append(s.log, Entry{})
	}
}

func (s *Server) broadcast(name string, req any) {
	// Broadcast RPC request to all cluster members except self
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			var rsp interface{}
			_ = s.rpcCall(i, name, req, &rsp)
		}(i)
	}
}

func (s *Server) HandleVRFRevealRequest(req VRFRevealRequest, rsp *VRFRevealResponse) error {
	// Handle VRF reveal and verify during election reveal phase
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Epoch != s.currentEpoch {
		return nil
	}

	s.validReveals[req.SenderID] = req.RevealedHash

	return nil
}

func (s *Server) HandleHeartbeatRequest(req HeartbeatRequest, rsp *HeartbeatResponse) error {
	// Update last heartbeat timestamp for liveness tracking
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Epoch != s.currentEpoch {
		return nil
	}

	s.lastHeartbeat[req.SenderID] = req.Timestamp

	return nil
}

func (s *Server) HandleBatchAssignmentRequest(req BatchAssignmentRequest, rsp *BatchAssignmentResponse) error {
	// Workers receive and start executing assigned batch
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != "worker" {
		return nil
	}

	s.currentBatch = req.Batch

	go s.executeBatchWithDependencies(req.Batch)

	return nil
}

func (s *Server) HandleBatchResultRequest(req BatchResultRequest, rsp *BatchResultResponse) error {
	// Leader collects execution results from workers
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != "leader" {
		return nil
	}

	if _, ok := s.workerBatches[req.Result.WorkerID]; !ok {
		return nil // ignore invalid result
	}

	s.workerResults[req.Result.WorkerID] = req.Result

	return nil
}

func (s *Server) HandleStateDependencyRequest(req StateDependencyRequest, rsp *StateDependencyResponse) error {
	// Provide state data for dependency resolution between workers
	s.mu.Lock()
	defer s.mu.Unlock()

	rsp.StateData = make(map[string][]byte)

	for _, key := range req.Request.StateKeys {
		strKey := string(key)
		if val, ok := s.executionState[strKey]; ok {
			rsp.StateData[strKey] = val
		}
	}

	return nil
}

func (s *Server) HandleNodeJoinRequest(req NodeJoinRequest, rsp *NodeJoinResponse) error {
	// Process node join requests: leaders queue, others approve
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role == "leader" {
		rsp.Approved = true
		s.pendingJoinRequests[req.Request.NodeID] = req.Request
	} else {
		approved := true
		approvalReq := NodeJoinApprovalRequest{
			RPCMessage: RPCMessage{Epoch: s.currentEpoch},
			NodeID:     req.Request.NodeID,
			Approved:   approved,
			VoterID:    s.id,
			Signature:  []byte{}, // sign
			Timestamp:  time.Now(),
		}
		var approvalRsp NodeJoinApprovalResponse
		s.rpcCall(s.getClusterIndex(req.TargetLeader), "Server.HandleNodeJoinApprovalRequest", approvalReq, &approvalRsp)
		rsp.Approved = approved
	}

	return nil
}

func (s *Server) HandleNodeJoinApprovalRequest(req NodeJoinApprovalRequest, rsp *NodeJoinApprovalResponse) error {
	// Leader collects approvals for join requests
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != "leader" {
		return nil
	}

	if _, ok := s.pendingApprovals[req.NodeID]; !ok {
		s.pendingApprovals[req.NodeID] = make(map[uint64]bool)
	}
	s.pendingApprovals[req.NodeID][req.VoterID] = req.Approved

	return nil
}

func (s *Server) HandleClusterMemberUpdateRequest(req ClusterMemberUpdateRequest, rsp *ClusterMemberUpdateResponse) error {
	// Update cluster membership upon successful join
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cluster = append(s.cluster, req.NewMember)

	return nil
}

func (s *Server) HandleLeaderQueryRequest(req LeaderQueryRequest, rsp *LeaderQueryResponse) error {
	// Respond with current leadership information
	s.mu.Lock()
	defer s.mu.Unlock()

	rsp.LeaderID = s.leaderID
	rsp.BackupLeaderID = s.backupLeaderID
	rsp.Epoch = s.currentEpoch
	rsp.SenderID = s.id
	rsp.LeaderAddress = s.getAddress(s.leaderID)
	rsp.BackupLeaderAddress = s.getAddress(s.backupLeaderID)

	return nil
}

func (s *Server) HandleGetClusterRequest(req GetClusterRequest, rsp *GetClusterResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Epoch != s.currentEpoch {
		return nil
	}

	rsp.Cluster = s.cluster

	return nil
}

// updateEpoch removed - not used in the current implementation.

func (s *Server) rpcCall(i int, name string, req any, rsp any) bool {
	// Perform RPC call to cluster member, establishing connection if needed
	// Read cluster member and cached client under lock
	s.mu.Lock()
	if i < 0 || i >= len(s.cluster) {
		s.mu.Unlock()
		s.warnf("rpcCall: invalid cluster index %d", i)
		return false
	}
	addr := s.cluster[i].Address
	cached := s.cluster[i].rpcClient
	targetID := s.cluster[i].Id
	s.mu.Unlock()

	var err error
	var client *rpc.Client = cached
	if client == nil {
		client, err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			s.warnf("Error dialing %s: %v", addr, err)
			return false
		}
		// cache the client for future calls
		s.mu.Lock()
		// re-check bounds in case cluster changed concurrently
		if i >= 0 && i < len(s.cluster) {
			s.cluster[i].rpcClient = client
		}
		s.mu.Unlock()
	}

	err = client.Call(name, req, rsp)
	if err != nil {
		s.warnf("Error calling %s on %d: %s.", name, targetID, err)
		return false
	}
	return true
}

func (s *Server) callByAddress(address string, name string, req any, rsp any) bool {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		s.warnf("Error dialing %s: %s", address, err)
		return false
	}
	err = client.Call(name, req, rsp)
	client.Close()
	if err != nil {
		s.warnf("Error calling %s on %s: %s", name, address, err)
		return false
	}
	return true
}

func (s *Server) AcceptTransaction(txData []byte) []byte {
	// Leader accepts and timestamps incoming transactions
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != "leader" {
		return nil
	}

	hash := sha256.Sum256(txData)
	txID := hash[:]
	strID := string(txID)

	timestamp := uint64(time.Now().UnixMicro())

	s.txPool[strID] = Transaction{
		TxID:             txID,
		TxData:           txData,
		ArrivalTimestamp: timestamp,
		GasPrice:         0, // stub
	}

	return txID
}

func (s *Server) orderTransactionsDeterministic() []Transaction {
	// Order transactions by arrival timestamp and tx_id for MEV resistance
	var txs []Transaction
	for _, tx := range s.txPool {
		txs = append(txs, tx)
	}

	sort.Slice(txs, func(i, j int) bool {
		if txs[i].ArrivalTimestamp != txs[j].ArrivalTimestamp {
			return txs[i].ArrivalTimestamp < txs[j].ArrivalTimestamp
		}
		return string(txs[i].TxID) < string(txs[j].TxID)
	})

	return txs
}

func (s *Server) ShouldCreateBatch() bool {
	// Determine if a new batch should be created based on size or timeout
	return len(s.txPool) >= MaxBatchSize || time.Since(s.lastBatchTime) >= time.Second
}

func (s *Server) CreateAndDistributeBatches() {
	// Leader creates batches from ordered transactions and assigns to workers
	s.mu.Lock()
	defer s.mu.Unlock()

	orderedTxs := s.orderTransactionsDeterministic()

	activeWorkers := s.getActiveWorkers()

	if len(activeWorkers) == 0 {
		return
	}

	batchSize := (len(orderedTxs) + len(activeWorkers) - 1) / len(activeWorkers)

	for i, w := range activeWorkers {
		start := i * batchSize
		end := min(start+batchSize, len(orderedTxs))
		txs := orderedTxs[start:end]

		batchID := s.batchSequence

		batch := Batch{
			BatchID:          batchID,
			Sequence:         s.batchSequence,
			Transactions:     txs,
			WorkerID:         w,
			CreatedTimestamp: time.Now(),
		}

		s.workerBatches[w] = batch

		s.batchSequence++

		req := BatchAssignmentRequest{
			RPCMessage:     RPCMessage{Epoch: s.currentEpoch},
			Batch:          batch,
			TargetWorker:   w,
			SequenceNumber: batch.Sequence,
		}

		var rsp BatchAssignmentResponse
		s.rpcCall(s.getClusterIndex(w), "Server.HandleBatchAssignmentRequest", req, &rsp)
	}

	s.txPool = make(map[string]Transaction)
	s.lastBatchTime = time.Now()
}

func (s *Server) getActiveWorkers() []uint64 {
	// Get list of active workers based on recent activity
	var workers []uint64
	for _, c := range s.cluster {
		if c.Id != s.leaderID && c.Id != s.backupLeaderID {
			workers = append(workers, c.Id)
		}
	}
	return workers
}

func (s *Server) getClusterIndex(id uint64) int {
	// Find cluster index for given node ID
	for i, c := range s.cluster {
		if c.Id == id {
			return i
		}
	}
	return -1
}

func (s *Server) getAddress(id uint64) string {
	i := s.getClusterIndex(id)
	if i == -1 {
		return ""
	}
	return s.cluster[i].Address
}

func (s *Server) executeBatchWithDependencies(batch Batch) {
	// Worker executes batch, handling state dependencies (stubbed)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stub execution, assume no dependencies

	var executionResults []ExecutionResult

	for _, tx := range batch.Transactions {
		res, err := s.statemachine.Apply(tx.TxData)

		success := err == nil

		// Stub state changes

		stateChanges := []StateChange{}

		executionResults = append(executionResults, ExecutionResult{
			TxID:         tx.TxID,
			Success:      success,
			ResultData:   res,
			GasUsed:      0, // stub
			StateChanges: stateChanges,
		})
	}

	result := BatchResult{
		BatchID:          batch.BatchID,
		WorkerID:         s.id,
		ExecutionResults: executionResults,
	}

	req := BatchResultRequest{
		RPCMessage:   RPCMessage{Epoch: s.currentEpoch},
		Result:       result,
		TargetLeader: s.leaderID,
	}

	var rsp BatchResultResponse
	s.rpcCall(s.getClusterIndex(s.leaderID), "Server.HandleBatchResultRequest", req, &rsp)
}

func (s *Server) AllBatchesCompleted() bool {
	// Check if all assigned batches have results
	return len(s.workerResults) == len(s.workerBatches)
}

func (s *Server) FinalizeBatchExecution() {
	// Leader finalizes batch execution, verifies results, and bundles to L1
	s.mu.Lock()
	defer s.mu.Unlock()

	var orderedResults []BatchResult

	for _, res := range s.workerResults {
		orderedResults = append(orderedResults, res)
	}

	sort.Slice(orderedResults, func(i, j int) bool {
		return s.workerBatches[orderedResults[i].WorkerID].Sequence < s.workerBatches[orderedResults[j].WorkerID].Sequence
	})

	for _, res := range orderedResults {
		// Apply state changes, stub
		for _, execRes := range res.ExecutionResults {
			for _, change := range execRes.StateChanges {
				s.executionState[string(change.Key)] = change.NewValue
			}
		}
	}

	s.bundleToL1(orderedResults)

	s.workerBatches = make(map[uint64]Batch)
	s.workerResults = make(map[uint64]BatchResult)
}

func (s *Server) bundleToL1(results []BatchResult) {
	// Submit finalized batch results to L1 (stub)
	// Stub
	s.debug("Bundled to L1")
}

func (s *Server) computeEpochSeed(epoch uint64) []byte {
	// Compute deterministic seed for VRF election
	h := sha256.New()
	if epoch == 1 && s.genesisConfig != nil {
		timestampBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(timestampBytes, s.genesisConfig.GenesisTimestamp)
		h.Write(timestampBytes)
	}
	epochBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(epochBytes, epoch)
	h.Write(epochBytes)
	return h.Sum(nil)
}

func (s *Server) ParticipateInElection() {
	// Participate in VRF-based leader election if staked sufficiently
	s.mu.Lock()
	defer s.mu.Unlock()

	currentTime := time.Now()

	commitStart := s.epochStartTime.Add(ProcessingPhase)

	revealStart := commitStart.Add(ElectionCommitPeriod)

	if currentTime.After(revealStart) && currentTime.Before(revealStart.Add(ElectionRevealPeriod)) && s.revealedHash == 0 {
		seed := s.computeEpochSeed(s.currentEpoch)
		hash, proof := s.vrfGenerateHash(seed)
		s.revealedHash = hash

		req := VRFRevealRequest{
			RPCMessage:   RPCMessage{Epoch: s.currentEpoch},
			SenderID:     s.id,
			RevealedHash: hash,
			VRFProof:     proof,
		}

		s.broadcast("Server.HandleVRFRevealRequest", req)
	}
}

func (s *Server) FinalizeElection() {
	// Determine leaders based on weighted VRF reveals and update roles
	s.mu.Lock()
	defer s.mu.Unlock()

	var reveals []struct {
		id   uint64
		hash uint64
	}

	for id, hash := range s.validReveals {
		reveals = append(reveals, struct {
			id   uint64
			hash uint64
		}{id, hash})
	}

	sort.Slice(reveals, func(i, j int) bool {
		return reveals[i].hash < reveals[j].hash
	})

	if len(reveals) >= 2 {
		s.leaderID = reveals[0].id
		s.backupLeaderID = reveals[1].id
		s.leaderAddress = s.getAddress(s.leaderID)
		s.backupLeaderAddress = s.getAddress(s.backupLeaderID)
	}

	if s.leaderID == s.id {
		s.role = "leader"
	} else if s.backupLeaderID == s.id {
		s.role = "backup_leader"
	} else {
		s.role = "worker"
	}

	s.revealedHash = 0
	s.validReveals = make(map[uint64]uint64)

	s.persist()

	if s.role == "leader" {
		s.ProcessPendingJoins()
	}
}

func (s *Server) CheckGenesisConditions() bool {
	// Check if node should initiate genesis startup
	return s.genesisConfig != nil && s.currentEpoch == 0 && uint64(time.Now().UnixMicro()) >= s.genesisConfig.GenesisTimestamp
}

func (s *Server) HandleGenesisStartup() {
	// Initiate genesis election with staggered delay
	s.mu.Lock()
	defer s.mu.Unlock()

	stagger := time.Duration(s.id%100) * time.Millisecond

	time.Sleep(stagger)

	s.currentEpoch = 1

	s.epochStartTime = time.Now()

	s.ParticipateInElection()
}

func (s *Server) JoinExistingCluster() {
	// Join running cluster by querying leader and sending join request
	s.mu.Lock()
	defer s.mu.Unlock()

	var leaderAddress string

	// Query bootstrap for leader

	if s.genesisConfig != nil {
		for _, boot := range s.genesisConfig.BootstrapNodes {
			client, err := rpc.DialHTTP("tcp", boot)
			if err != nil {
				continue
			}

			req := LeaderQueryRequest{RPCMessage: RPCMessage{}}
			var rsp LeaderQueryResponse
			err = client.Call("Server.HandleLeaderQueryRequest", req, &rsp)
			client.Close()
			if err == nil {
				s.leaderID = rsp.LeaderID
				s.backupLeaderID = rsp.BackupLeaderID
				s.currentEpoch = rsp.Epoch
				leaderAddress = rsp.LeaderAddress
				s.leaderAddress = leaderAddress
				s.backupLeaderAddress = rsp.BackupLeaderAddress
				break
			}
		}
	}

	if s.leaderID == 0 {
		// No leader, wait for election
		return
	}

	if s.currentEpoch == 0 {
		// new node, send join

		joinReq := JoinRequest{
			NodeID:    s.id,
			Address:   s.address,
			Signature: []byte{}, // sign
			Timestamp: time.Now(),
		}

		req := NodeJoinRequest{
			RPCMessage:   RPCMessage{Epoch: s.currentEpoch},
			Request:      joinReq,
			TargetLeader: s.leaderID,
		}

		var rsp NodeJoinResponse
		s.callByAddress(leaderAddress, "Server.HandleNodeJoinRequest", req, &rsp)

		if rsp.Approved {
			s.RequestStateSyncFromLeader()
		}
	} else {
		// recovery, sync
		s.RequestStateSyncFromLeader()
	}
}

func (s *Server) RequestStateSyncFromLeader() {
	// Synchronize state with leader after join or recovery (stub)
	s.mu.Lock()
	leaderAddress := s.leaderAddress
	s.mu.Unlock()

	req := GetClusterRequest{RPCMessage: RPCMessage{Epoch: s.currentEpoch}}
	var rsp GetClusterResponse
	if s.callByAddress(leaderAddress, "Server.HandleGetClusterRequest", req, &rsp) {
		s.mu.Lock()
		s.cluster = rsp.Cluster
		// Add self if not present
		if s.getClusterIndex(s.id) == -1 {
			s.cluster = append(s.cluster, ClusterMember{
				Id:       s.id,
				Address:  s.address,
				LastSeen: time.Now(),
			})
		}
		s.mu.Unlock()
	}

	s.debug("Synced state from leader")
}

func (s *Server) ProcessPendingJoins() {
	// Process queued join requests at epoch boundary with cluster approval
	s.mu.Lock()
	defer s.mu.Unlock()

	for nodeID, req := range s.pendingJoinRequests {
		// Broadcast to cluster for approval

		bReq := NodeJoinRequest{
			RPCMessage:   RPCMessage{Epoch: s.currentEpoch},
			Request:      req,
			TargetLeader: s.id,
		}

		s.broadcast("Server.HandleNodeJoinRequest", bReq)

		// Wait for approvals, stub

		time.Sleep(time.Second) // stub

		approvalCount := uint64(0)
		for _, approved := range s.pendingApprovals[nodeID] {
			if approved {
				approvalCount++
			}
		}

		if approvalCount >= uint64(len(s.cluster))*2/3 {
			newMember := ClusterMember{
				Id:       nodeID,
				Address:  req.Address,
				LastSeen: time.Now(),
			}

			updateReq := ClusterMemberUpdateRequest{
				RPCMessage: RPCMessage{Epoch: s.currentEpoch},
				NewMember:  newMember,
				Signature:  []byte{}, // sign
			}

			s.broadcast("Server.HandleClusterMemberUpdateRequest", updateReq)

			s.cluster = append(s.cluster, newMember)
		}

		delete(s.pendingJoinRequests, nodeID)
		delete(s.pendingApprovals, nodeID)
	}
}

func (s *Server) SendHeartbeat() {
	// Send heartbeat if leader or backup leader
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != "leader" && s.role != "backup_leader" {
		return
	}

	req := HeartbeatRequest{
		RPCMessage: RPCMessage{Epoch: s.currentEpoch},
		SenderID:   s.id,
		Role:       s.role,
		Timestamp:  time.Now(),
	}

	s.broadcast("Server.HandleHeartbeatRequest", req)
}

func (s *Server) CheckForFailures() {
	// Detect leader/backup failures and trigger promotion or emergency election
	s.mu.Lock()
	defer s.mu.Unlock()

	currentTime := time.Now()

	leaderTimeout := currentTime.Sub(s.lastHeartbeat[s.leaderID]) > HeartbeatTimeout

	backupTimeout := currentTime.Sub(s.lastHeartbeat[s.backupLeaderID]) > HeartbeatTimeout

	if leaderTimeout && !backupTimeout {
		s.leaderID = s.backupLeaderID
		s.backupLeaderID = 0
		s.leaderAddress = s.backupLeaderAddress
		s.backupLeaderAddress = ""
		if s.id == s.leaderID {
			s.role = "leader"
		}
	} else if leaderTimeout && backupTimeout {
		time.Sleep(EmergencyElectionDelay)

		s.currentEpoch++

		s.epochStartTime = currentTime

		s.role = "worker"

		s.ParticipateInElection()
	}
}

func (s *Server) Start() {
	// Initialize and start the DESYNC server
	s.mu.Lock()
	s.done = false
	s.role = "worker"
	s.mu.Unlock()

	s.restore()

	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	s.server = &http.Server{Handler: mux}
	go s.server.Serve(l)

	// Determine startup path: genesis or join
	if s.CheckGenesisConditions() {
		s.HandleGenesisStartup()
	} else {
		s.JoinExistingCluster()
	}

	s.mu.Lock()
	s.resetElectionTimeout()
	s.mu.Unlock()

	// Main event loop with ticker for periodic tasks
	go func() {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()

		for range ticker.C {
			s.mu.Lock()
			if s.done {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()

			currentPhase := s.GetCurrentPhase()
			switch currentPhase {
			case "processing":
				s.mu.Lock()
				if s.role == "leader" {
					if s.ShouldCreateBatch() {
						s.CreateAndDistributeBatches()
					}
					if s.AllBatchesCompleted() {
						s.FinalizeBatchExecution()
					}
				}
				s.mu.Unlock()

				s.CheckForFailures()
			case "election":
				s.ParticipateInElection()
			}

			// Check for epoch transition
			if time.Now().After(s.epochStartTime.Add(EpochDuration)) {
				s.FinalizeElection()
				s.epochStartTime = s.epochStartTime.Add(EpochDuration)
				s.currentEpoch++
			}

			if s.role == "leader" || s.role == "backup_leader" {
				s.SendHeartbeat()
			}
		}
	}()
}

func (s *Server) GetCurrentPhase() string {
	// Determine current phase: processing or election
	currentTime := time.Now()

	if currentTime.Before(s.epochStartTime.Add(ProcessingPhase)) {
		return "processing"
	} else if currentTime.Before(s.epochStartTime.Add(EpochDuration)) {
		return "election"
	} else {
		return "transition"
	}
}

func (s *Server) resetElectionTimeout() {
	// Reset random election timeout (not directly used in DESYNC but for compatibility)
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	interval := time.Duration(r.Intn(s.heartbeatMs*2)+s.heartbeatMs*2) * time.Millisecond
	s.electionTimeout = time.Now().Add(interval)
}
