# DESYNC Protocol Implementation

This document describes the DESYNC protocol, a distributed consensus system implemented in Go, designed for high-throughput, MEV-resistant transaction processing with leader election via Verifiable Random Functions (VRF). It includes epoch-based processing, transaction batching, worker execution, and fault tolerance mechanisms.

## Imports

```go
import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math/big"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)
```

## Constants

```go
const (
	EpochDuration         = 180 * time.Second
	ProcessingPhase       = 120 * time.Second
	ElectionPhase         = 60 * time.Second
	ElectionCommitPeriod  = 40 * time.Second
	ElectionRevealPeriod  = 20 * time.Second
	HeartbeatInterval     = 100 * time.Millisecond
	HeartbeatTimeout      = 500 * time.Millisecond
	WorkerExecutionTimeout = 30 * time.Second
	EmergencyElectionDelay = 1 * time.Second
	MaxBatchSize          = 1000
	MinParticipationThreshold = "2/3"
	GenesisQuorumTimeout  = 10 * time.Second
	NodeJoinTimeout       = 10 * time.Second
)
```

## Utility Functions

### Assert

```go
func Assert[T comparable](msg string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}
```

### Min/Max

```go
func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T ~int | ~uint64](a, b T) T {
	if a > b {
		return a
	}
	return b
}
```

## Data Structures

### Interfaces and Structs

```go
type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

type ApplyResult struct {
	Result []byte
	Error  error
}

type Entry struct {
	Epoch     uint64
	Index     uint64
	TxID      []byte
	TxData    []byte
	Timestamp uint64
	StateRoot []byte
}

type Transaction struct {
	TxID             []byte
	TxData           []byte
	ArrivalTimestamp uint64
	GasPrice         uint64
}

type Batch struct {
	BatchID          uint64
	Sequence         uint64
	Transactions     []Transaction
	WorkerID         uint64
	CreatedTimestamp time.Time
}

type ExecutionResult struct {
	TxID         []byte
	Success      bool
	ResultData   []byte
	GasUsed      uint64
	StateChanges []StateChange
}

type StateChange struct {
	Key      []byte
	OldValue []byte
	NewValue []byte
}

type BatchResult struct {
	BatchID          uint64
	WorkerID         uint64
	ExecutionResults []ExecutionResult
	NewStateRoot     []byte
	Signature        []byte
	ExecutionProof   []byte
}

type StateRequest struct {
	RequestingWorker uint64
	StateKeys        [][]byte
	BatchID          uint64
	Timestamp        time.Time
}

type JoinRequest struct {
	NodeID    uint64
	Address   string
	PublicKey []byte
	Stake     uint64
	Signature []byte
	Timestamp time.Time
}

type GenesisConfig struct {
	ClusterMembers   []ClusterMember
	GenesisTimestamp uint64
	MinimumStake     uint64
	TotalStake       uint64
	EpochDuration    string
	BootstrapNodes   []string
}

type RPCMessage struct {
	Epoch uint64
}

// RPC Request/Response Types
type VRFCommitRequest struct {
	RPCMessage
	SenderID   uint64
	HashCommit []byte
}

type VRFCommitResponse struct {
	RPCMessage
}

type VRFRevealRequest struct {
	RPCMessage
	SenderID     uint64
	RevealedHash uint64
	VRFProof     []byte
}

type VRFRevealResponse struct {
	RPCMessage
}

type HeartbeatRequest struct {
	RPCMessage
	SenderID  uint64
	Role      string
	Timestamp time.Time
}

type HeartbeatResponse struct {
	RPCMessage
}

type BatchAssignmentRequest struct {
	RPCMessage
	Batch          Batch
	TargetWorker   uint64
	SequenceNumber uint64
}

type BatchAssignmentResponse struct {
	RPCMessage
}

type BatchResultRequest struct {
	RPCMessage
	Result       BatchResult
	TargetLeader uint64
}

type BatchResultResponse struct {
	RPCMessage
}

type StateDependencyRequest struct {
	RPCMessage
	Request      StateRequest
	TargetWorker uint64
}

type StateDependencyResponse struct {
	RPCMessage
	StateData    map[string][]byte
	TargetWorker uint64
	RequestID    uint64
}

type NodeJoinRequest struct {
	RPCMessage
	Request      JoinRequest
	TargetLeader uint64
}

type NodeJoinResponse struct {
	RPCMessage
	Approved bool
}

type NodeJoinApprovalRequest struct {
	RPCMessage
	NodeID    uint64
	Approved  bool
	VoterID   uint64
	Signature []byte
	Timestamp time.Time
}

type NodeJoinApprovalResponse struct {
	RPCMessage
}

type ClusterMemberUpdateRequest struct {
	RPCMessage
	NewMember  ClusterMember
	TotalStake uint64
	Signature  []byte
}

type ClusterMemberUpdateResponse struct {
	RPCMessage
}

type FraudProofRequest struct {
	RPCMessage
	DisputingNode  uint64
	DisputedTxID   []byte
	ClaimedResult  ExecutionResult
	CorrectResult  ExecutionResult
	ExecutionProof []byte
	Timestamp      time.Time
}

type FraudProofResponse struct {
	RPCMessage
}

type LeaderQueryRequest struct {
	RPCMessage
}

type LeaderQueryResponse struct {
	RPCMessage
	LeaderID       uint64
	BackupLeaderID uint64
	Epoch          uint64
	SenderID       uint64
}

type ClusterMember struct {
	Id         uint64
	Address    string
	PublicKey  *ecdsa.PublicKey
	Stake      uint64
	LastSeen   time.Time
	nextIndex  uint64
	matchIndex uint64
	rpcClient  *rpc.Client
}

type ServerState string

const (
	leaderState       ServerState = "leader"
	backupLeaderState ServerState = "backup_leader"
	workerState       ServerState = "worker"
)
```

### Server Struct

```go
type Server struct {
	done            bool
	server          *http.Server
	Debug           bool
	mu              sync.Mutex
	currentEpoch    uint64
	log             []Entry
	commitIndex     uint64
	lastApplied     uint64
	state           ServerState
	id              uint64
	address         string
	electionTimeout time.Time
	heartbeatMs     int
	heartbeatTimeout time.Time
	statemachine    StateMachine
	metadataDir     string
	fd              *os.File
	cluster         []ClusterMember
	clusterIndex    int
	privateKey      *ecdsa.PrivateKey
	publicKey       *ecdsa.PublicKey
	votedHashCommit []byte
	revealedHash    uint64
	vrfProof        []byte
	stake           uint64
	stateRoot       []byte
	genesisConfig   *GenesisConfig
	role            string
	leaderID        uint64
	backupLeaderID  uint64
	epochStartTime  time.Time
	lastHeartbeat   map[uint64]time.Time
	txPool          map[string]Transaction
	workerBatches   map[uint64]Batch
	workerResults   map[uint64]BatchResult
	batchSequence   uint64
	pendingJoinRequests map[uint64]JoinRequest
	currentBatch    Batch
	executionState  map[string][]byte
	dependencyRequests []StateRequest
	pendingCommits  map[uint64][]byte
	validReveals    map[uint64]uint64
	pendingApprovals map[uint64]map[uint64]bool
	lastBatchTime   time.Time
}
```

## Server Methods

### Debugging

```go
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
```

### Initialization

```go
func NewServer(
	clusterConfig []ClusterMember,
	genesisConfig *GenesisConfig,
	statemachine StateMachine,
	metadataDir string,
	clusterIndex int,
) *Server {
	var cluster []ClusterMember
	for _, c := range clusterConfig {
		if c.Id == 0 {
			panic("Id must not be 0.")
		}
		cluster = append(cluster, c)
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
		pendingCommits:      make(map[uint64][]byte),
		validReveals:        make(map[uint64]uint64),
		pendingApprovals:    make(map[uint64]map[uint64]bool),
	}

	if s.genesisConfig != nil && len(s.genesisConfig.ClusterMembers) > clusterIndex {
		s.stake = s.genesisConfig.ClusterMembers[clusterIndex].Stake
		s.publicKey = s.cluster[clusterIndex].PublicKey
	}

	return s
}
```

### VRF Functions

```go
func (s *Server) vrfGenerateHash(seed []byte, priv *ecdsa.PrivateKey) (hash uint64, proof []byte) {
	h := sha256.Sum256(seed)
	r, sBig, err := ecdsa.Sign(rand.Reader, priv, h[:])
	if err != nil {
		panic(err)
	}
	proof = append(r.Bytes(), sBig.Bytes()...)
	hashH := sha256.Sum256(proof)
	hash = binary.LittleEndian.Uint64(hashH[:8])
	return
}

func (s *Server) vrfVerifyHash(hash uint64, proof []byte, pub *ecdsa.PublicKey, seed []byte) bool {
	if len(proof) != 64 {
		return false
	}
	r := new(big.Int).SetBytes(proof[:32])
	sBig := new(big.Int).SetBytes(proof[32:])
	h := sha256.Sum256(seed)
	if !ecdsa.Verify(pub, h[:], r, sBig) {
		return false
	}
	hashH := sha256.Sum256(proof)
	computedHash := binary.LittleEndian.Uint64(hashH[:8])
	return computedHash == hash
}
```

### Persistence

```go
type PersistentState struct {
	CurrentEpoch    uint64
	PrivateKey      *ecdsa.PrivateKey
	VotedHashCommit []byte
	RevealedHash    uint64
	Stake           uint64
	StateRoot       []byte
	GenesisConfig   *GenesisConfig
	Log             []Entry
	Cluster         []ClusterMember
}

func (s *Server) persist() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fd.Seek(0, 0)
	if err := s.fd.Truncate(0); err != nil {
		panic(err)
	}
	enc := gob.NewEncoder(s.fd)
	err := enc.Encode(PersistentState{
		CurrentEpoch:    s.currentEpoch,
		PrivateKey:      s.privateKey,
		VotedHashCommit: s.votedHashCommit,
		RevealedHash:    s.revealedHash,
		Stake:           s.stake,
		StateRoot:       s.stateRoot,
		GenesisConfig:   s.genesisConfig,
		Log:             s.log,
		Cluster:         s.cluster,
	})
	if err != nil {
		panic(err)
	}
	s.fd.Sync()
	s.debugf("Persisted state.")
}

func (s *Server) restore() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fd == nil {
		var err error
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, fmt.Sprintf("md_%d.dat", s.id)),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0755)
		if err != nil {
			panic(err)
		}
	}
	dec := gob.NewDecoder(s.fd)
	var ps PersistentState
	err := dec.Decode(&ps)
	if err == io.EOF {
		if s.privateKey == nil {
			priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				panic(err)
			}
			s.privateKey = priv
			s.publicKey = &priv.PublicKey
		}
		s.log = []Entry{}
		s.ensureLog()
		s.persist()
		return
	} else if err != nil {
		panic(err)
	}
	s.currentEpoch = ps.CurrentEpoch
	s.privateKey = ps.PrivateKey
	s.publicKey = &ps.PrivateKey.PublicKey
	s.votedHashCommit = ps.VotedHashCommit
	s.revealedHash = ps.RevealedHash
	s.stake = ps.Stake
	s.stateRoot = ps.StateRoot
	s.genesisConfig = ps.GenesisConfig
	s.log = ps.Log
	s.cluster = ps.Cluster
	s.ensureLog()
}

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		s.log = append(s.log, Entry{})
	}
}
```

### RPC Handlers

```go
func (s *Server) broadcast(name string, req any) {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}
		go func(i int) {
			var rsp interface{}
			ok := s.rpcCall(i, name, req, &rsp)
			if !ok {
				// retry later
			}
		}(i)
	}
}

func (s *Server) HandleVRFCommitRequest(req VRFCommitRequest, rsp *VRFCommitResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Epoch != s.currentEpoch {
		return nil
	}
	s.pendingCommits[req.SenderID] = req.HashCommit
	return nil
}

func (s *Server) HandleVRFRevealRequest(req VRFRevealRequest, rsp *VRFRevealResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Epoch != s.currentEpoch {
		return nil
	}
	commit, ok := s.pendingCommits[req.SenderID]
	if !ok {
		return nil
	}
	pub := s.getPublicKey(req.SenderID)
	if pub == nil {
		return nil
	}
	seed := s.computeEpochSeed(s.currentEpoch)
	if s.vrfVerifyHash(req.RevealedHash, req.VRFProof, pub, seed) {
		hashBi := big.NewInt(0).SetUint64(req.RevealedHash)
		idBi := big.NewInt(0).SetUint64(req.SenderID)
		epochBi := big.NewInt(0).SetUint64(req.Epoch)
		commitH := sha256.New()
		commitH.Write(hashBi.Bytes())
		commitH.Write(idBi.Bytes())
		commitH.Write(epochBi.Bytes())
		expectedCommit := commitH.Sum(nil)
		if string(expectedCommit) == string(commit) {
			s.validReveals[req.SenderID] = req.RevealedHash
		}
	}
	return nil
}

func (s *Server) HandleHeartbeatRequest(req HeartbeatRequest, rsp *HeartbeatResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Epoch != s.currentEpoch {
		return nil
	}
	s.lastHeartbeat[req.SenderID] = req.Timestamp
	return nil
}

func (s *Server) HandleBatchAssignmentRequest(req BatchAssignmentRequest, rsp *BatchAssignmentResponse) error {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != "leader" {
		return nil
	}
	s.workerResults[req.Result.WorkerID] = req.Result
	return nil
}

func (s *Server) HandleStateDependencyRequest(req StateDependencyRequest, rsp *StateDependencyResponse) error {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role == "leader" {
		if s.verifyJoinSignature(req.Request) && s.genesisConfig != nil && req.Request.Stake >= s.genesisConfig.MinimumStake {
			s.pendingJoinRequests[req.Request.NodeID] = req.Request
			rsp.Approved = true
		} else {
			rsp.Approved = false
		}
	} else {
		approved := s.verifyJoinSignature(req.Request) && s.genesisConfig != nil && req.Request.Stake >= s.genesisConfig.MinimumStake
		approvalReq := NodeJoinApprovalRequest{
			RPCMessage: RPCMessage{Epoch: s.currentEpoch},
			NodeID:     req.Request.NodeID,
			Approved:   approved,
			VoterID:    s.id,
			Signature:  []byte{},
			Timestamp:  time.Now(),
		}
		var approvalRsp NodeJoinApprovalResponse
		s.rpcCall(s.getClusterIndex(req.TargetLeader), "Server.HandleNodeJoinApprovalRequest", approvalReq, &approvalRsp)
		rsp.Approved = approved
	}
	return nil
}

func (s *Server) HandleNodeJoinApprovalRequest(req NodeJoinApprovalRequest, rsp *NodeJoinApprovalResponse) error {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cluster = append(s.cluster, req.NewMember)
	if s.genesisConfig != nil {
		s.genesisConfig.TotalStake = req.TotalStake
	}
	return nil
}

func (s *Server) HandleFraudProofRequest(req FraudProofRequest, rsp *FraudProofResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nil
}

func (s *Server) HandleLeaderQueryRequest(req LeaderQueryRequest, rsp *LeaderQueryResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rsp.LeaderID = s.leaderID
	rsp.BackupLeaderID = s.backupLeaderID
	rsp.Epoch = s.currentEpoch
	rsp.SenderID = s.id
	return nil
}

func (s *Server) updateEpoch(msg RPCMessage) bool {
	if msg.Epoch > s.currentEpoch {
		s.currentEpoch = msg.Epoch
		s.role = "worker"
		return true
	}
	return false
}

func (s *Server) rpcCall(i int, name string, req any, rsp any) bool {
	s.mu.Lock()
	c := s.cluster[i]
	var err error
	var rpcClient *rpc.Client = c.rpcClient
	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
		rpcClient = c.rpcClient
	}
	s.mu.Unlock()
	if err == nil {
		err = rpcClient.Call(name, req, rsp)
	}
	if err != nil {
		s.warnf("Error calling %s on %d: %s.", name, c.Id, err)
	}
	return err == nil
}
```

### Transaction Processing

```go
func (s *Server) AcceptTransaction(txData []byte) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != "leader" {
		return nil
	}
	txID := sha256.Sum256(txData)
	strID := string(txID[:])
	timestamp := uint64(time.Now().UnixMicro())
	s.txPool[strID] = Transaction{
		TxID:             txID[:],
		TxData:           txData,
		ArrivalTimestamp: timestamp,
		GasPrice:         0,
	}
	return txID[:]
}

func (s *Server) orderTransactionsDeterministic() []Transaction {
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
	return len(s.txPool) >= MaxBatchSize || time.Since(s.lastBatchTime) >= time.Second
}

func (s *Server) CreateAndDistributeBatches() {
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
```

### Helper Functions

```go
func (s *Server) getActiveWorkers() []uint64 {
	var workers []uint64
	for _, c := range s.cluster {
		if c.Id != s.leaderID && c.Id != s.backupLeaderID && time.Since(c.LastSeen) < HeartbeatTimeout*5 {
			workers = append(workers, c.Id)
		}
	}
	return workers
}

func (s *Server) getClusterIndex(id uint64) int {
	for i, c := range s.cluster {
		if c.Id == id {
			return i
		}
	}
	return -1
}

func (s *Server) getPublicKey(id uint64) *ecdsa.PublicKey {
	i := s.getClusterIndex(id)
	if i == -1 {
		return nil
	}
	return s.cluster[i].PublicKey
}
```

### Batch Execution

```go
func (s *Server) executeBatchWithDependencies(batch Batch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var executionResults []ExecutionResult
	newState := make(map[string][]byte, len(s.executionState))
	for k, v := range s.executionState {
		newState[k] = v
	}
	for _, tx := range batch.Transactions {
		res, err := s.statemachine.Apply(tx.TxData)
		success := err == nil
		stateChanges := []StateChange{}
		executionResults = append(executionResults, ExecutionResult{
			TxID:         tx.TxID,
			Success:      success,
			ResultData:   res,
			GasUsed:      0,
			StateChanges: stateChanges,
		})
	}
	newStateRoot := s.computeStateRoot([]StateChange{})
	result := BatchResult{
		BatchID:          batch.BatchID,
		WorkerID:         s.id,
		ExecutionResults: executionResults,
		NewStateRoot:     newStateRoot,
		Signature:        []byte{},
		ExecutionProof:   []byte{},
	}
	req := BatchResultRequest{
		RPCMessage:   RPCMessage{Epoch: s.currentEpoch},
		Result:       result,
		TargetLeader: s.leaderID,
	}
	var rsp BatchResultResponse
	s.rpcCall(s.getClusterIndex(s.leaderID), "Server.HandleBatchResultRequest", req, &rsp)
}

func (s *Server) computeStateRoot(changes []StateChange) []byte {
	h := sha256.New()
	return h.Sum(nil)
}

func (s *Server) AllBatchesCompleted() bool {
	return len(s.workerResults) == len(s.workerBatches)
}

func (s *Server) FinalizeBatchExecution() {
	s.mu.Lock()
	defer s.mu.Unlock()
	var orderedResults []BatchResult
	for _, res := range s.workerResults {
		orderedResults = append(orderedResults, res)
	}
	sort.Slice(orderedResults, func(i, j int) bool {
		return s.workerBatches[orderedResults[i].WorkerID].Sequence < s.workerBatches[orderedResults[j].WorkerID].Sequence
	})
	currentRoot := s.stateRoot
	for _, res := range orderedResults {
		computedRoot := s.computeStateRoot(res.ExecutionResults[0].StateChanges)
		if string(computedRoot) != string(res.NewStateRoot) {
			s.slashStake(res.WorkerID, "invalid_execution")
		}
		currentRoot = res.NewStateRoot
	}
	s.stateRoot = currentRoot
	s.bundleToL1(orderedResults)
	s.workerBatches = make(map[uint64]Batch)
	s.workerResults = make(map[uint64]BatchResult)
}

func (s *Server) bundleToL1(results []BatchResult) {
	s.debug("Bundled to L1")
}

func (s *Server) slashStake(nodeID uint64, violation string) {
	s.debugf("Slashed node %d for %s", nodeID, violation)
}
```

### Election and Cluster Management

```go
func (s *Server) computeEpochSeed(epoch uint64) []byte {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.genesisConfig == nil || s.stake < s.genesisConfig.MinimumStake {
		return
	}
	currentTime := time.Now()
	commitStart := s.epochStartTime.Add(ProcessingPhase)
	if currentTime.Before(commitStart.Add(ElectionCommitPeriod)) && s.votedHashCommit == nil {
		seed := s.computeEpochSeed(s.currentEpoch)
		hash, proof := s.vrfGenerateHash(seed, s.privateKey)
		hashBi := big.NewInt(0).SetUint64(hash)
		idBi := big.NewInt(0).SetUint64(s.id)
		epochBi := big.NewInt(0).SetUint64(s.currentEpoch)
		commitH := sha256.New()
		commitH.Write(hashBi.Bytes())
		commitH.Write(idBi.Bytes())
		commitH.Write(epochBi.Bytes())
		commit := commitH.Sum(nil)
		s.votedHashCommit = commit
		s.revealedHash = hash
		s.vrfProof = proof
		s.persist()
		req := VRFCommitRequest{
			RPCMessage: RPCMessage{Epoch: s.currentEpoch},
			SenderID:   s.id,
			HashCommit: commit,
		}
		s.broadcast("Server.HandleVRFCommitRequest", req)
	}
	revealStart := commitStart.Add(ElectionCommitPeriod)
	if currentTime.After(revealStart) && currentTime.Before(revealStart.Add(ElectionRevealPeriod)) && s.votedHashCommit != nil && s.revealedHash != 0 {
		req := VRFRevealRequest{
			RPCMessage:   RPCMessage{Epoch: s.currentEpoch},
			SenderID:     s.id,
			RevealedHash: s.revealedHash,
			VRFProof:     s.vrfProof,
		}
		s.broadcast("Server.HandleVRFRevealRequest", req)
	}
}

func (s *Server) FinalizeElection() {
	s.mu.Lock()
	defer s.mu.Unlock()
	participatingStake := uint64(0)
	for id := range s.validReveals {
		participatingStake += s.cluster[s.getClusterIndex(id)].Stake
	}
	if s.genesisConfig != nil && participatingStake < (s.genesisConfig.TotalStake*2/3) {
		return
	}
	var reveals []struct {
		id    uint64
		hash  uint64
		stake uint64
	}
	for id, hash := range s.validReveals {
		reveals = append(reveals, struct {
			id    uint64
			hash  uint64
			stake uint64
		}{id, hash, s.cluster[s.getClusterIndex(id)].Stake})
	}
	sort.Slice(reveals, func(i, j int) bool {
		left := new(big.Int).Mul(big.NewInt(0).SetUint64(reveals[i].hash), big.NewInt(0).SetUint64(reveals[j].stake))
		right := new(big.Int).Mul(big.NewInt(0).SetUint64(reveals[j].hash), big.NewInt(0).SetUint64(reveals[i].stake))
		return left.Cmp(right) < 0
	})
	if len(reveals) >= 2 {
		s.leaderID = reveals[0].id
		s.backupLeaderID = reveals[1].id
	}
	if s.leaderID == s.id {
		s.role = "leader"
	} else if s.backupLeaderID == s.id {
		s.role = "backup_leader"
	} else {
		s.role = "worker"
	}
	s.votedHashCommit = nil
	s.revealedHash = 0
	s.vrfProof = nil
	s.pendingCommits = make(map[uint64][]byte)
	s.validReveals = make(map[uint64]uint64)
	s.persist()
	if s.role == "leader" {
		s.ProcessPendingJoins()
	}
}

func (s *Server) CheckGenesisConditions() bool {
	return s.genesisConfig != nil && s.currentEpoch == 0 && uint64(time.Now().UnixMicro()) >= s.genesisConfig.GenesisTimestamp
}

func (s *Server) HandleGenesisStartup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	stagger := time.Duration(s.id%100) * time.Millisecond
	time.Sleep(stagger)
	s.currentEpoch = 1
	s.epochStartTime = time.Now()
	s.ParticipateInElection()
}

func (s *Server) JoinExistingCluster() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.genesisConfig != nil {
		for _, boot := range s.genesisConfig.BootstrapNodes {
			client, err := rpc.DialHTTP("tcp", boot)
			if err != nil {
				continue
			}
			req := LeaderQueryRequest{RPCMessage: RPCMessage{}}
			var rsp LeaderQueryResponse
			err = client.Call("Server.HandleLeaderQueryRequest", req, &rsp)
			if err == nil {
				s.leaderID = rsp.LeaderID
				s.backupLeaderID = rsp.BackupLeaderID
				s.currentEpoch = rsp.Epoch
				break
			}
		}
	}
	if s.leaderID == 0 {
		return
	}
	if s.currentEpoch == 0 {
		joinReq := JoinRequest{
			NodeID:    s.id,
			Address:   s.address,
			PublicKey: s.publicKeyBytes(),
			Stake:     s.stake,
			Signature: []byte{},
			Timestamp: time.Now(),
		}
		req := NodeJoinRequest{
			RPCMessage:   RPCMessage{Epoch: s.currentEpoch},
			Request:      joinReq,
			TargetLeader: s.leaderID,
		}
		var rsp NodeJoinResponse
		s.rpcCall(s.getClusterIndex(s.leaderID), "Server.HandleNodeJoinRequest", req, &rsp)
		if rsp.Approved {
			s.RequestStateSyncFromLeader()
		}
	} else {
		s.RequestStateSyncFromLeader()
	}
}

func (s *Server) RequestStateSyncFromLeader() {
	s.debug("Synced state from leader")
}

func (s *Server) verifyJoinSignature(request JoinRequest) bool {
	return true
}

func (s *Server) ProcessPendingJoins() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for nodeID, req := range s.pendingJoinRequests {
		bReq := NodeJoinRequest{
			RPCMessage:   RPCMessage{Epoch: s.currentEpoch},
			Request:      req,
			TargetLeader: s.id,
		}
		s.broadcast("Server.HandleNodeJoinRequest", bReq)
		time.Sleep(time.Second)
		approvalStake := uint64(0)
		for voter, approved := range s.pendingApprovals[nodeID] {
			if approved {
				approvalStake += s.cluster[s.getClusterIndex(voter)].Stake
			}
		}
		if s.genesisConfig != nil && approvalStake >= s.genesisConfig.TotalStake*2/3 {
			newMember := ClusterMember{
				Id:        nodeID,
				Address:   req.Address,
				PublicKey: s.parsePublicKey(req.PublicKey),
				Stake:     req.Stake,
				LastSeen:  time.Now(),
			}
			updateReq := ClusterMemberUpdateRequest{
				RPCMessage: RPCMessage{Epoch: s.currentEpoch},
				NewMember:  newMember,
				TotalStake: s.genesisConfig.TotalStake + req.Stake,
				Signature:  []byte{},
			}
			s.broadcast("Server.HandleClusterMemberUpdateRequest", updateReq)
			s.cluster = append(s.cluster, newMember)
			s.genesisConfig.TotalStake += req.Stake
		}
		delete(s.pendingJoinRequests, nodeID)
		delete(s.pendingApprovals, nodeID)
	}
}

func (s *Server) parsePublicKey(pk []byte) *ecdsa.PublicKey {
	return nil
}

func (s *Server) publicKeyBytes() []byte {
	return []byte{}
}
```

### Heartbeat and Failure Detection

```go
func (s *Server) SendHeartbeat() {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	currentTime := time.Now()
	leaderTimeout := currentTime.Sub(s.lastHeartbeat[s.leaderID]) > HeartbeatTimeout
	backupTimeout := currentTime.Sub(s.lastHeartbeat[s.backupLeaderID]) > HeartbeatTimeout
	if leaderTimeout && !backupTimeout {
		s.leaderID = s.backupLeaderID
		s.backupLeaderID = 0
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
```

### Server Startup

```go
func (s *Server) Start() {
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
	if s.CheckGenesisConditions() {
		s.HandleGenesisStartup()
	} else {
		s.JoinExistingCluster()
	}
	s.mu.Lock()
	s.resetElectionTimeout()
	s.mu.Unlock()
	go func() {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.mu.Lock()
				if s.done {
					s.mu.Unlock()
					return
				}
				s.mu.Unlock()
				currentPhase := s.GetCurrentPhase()
				if currentPhase == "processing" {
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
				} else if currentPhase == "election" {
					s.ParticipateInElection()
				}
				if time.Now().After(s.epochStartTime.Add(EpochDuration)) {
					s.FinalizeElection()
					s.epochStartTime = s.epochStartTime.Add(EpochDuration)
					s.currentEpoch++
				}
				if s.role == "leader" || s.role == "backup_leader" {
					s.SendHeartbeat()
				}
			}
		}
	}()
}

func (s *Server) GetCurrentPhase() string {
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
	interval := time.Duration(mathrand.Intn(s.heartbeatMs*2)+s.heartbeatMs*2) * time.Millisecond
	s.electionTimeout = time.Now().Add(interval)
}
```

## Notes

- The DESYNC protocol uses epochs with processing and election phases for transaction handling and leader election.
- VRF-based leader election ensures randomness and fairness, weighted by stake.
- Transactions are batched and distributed to workers for parallel execution.
- Fault tolerance is achieved through heartbeats, backup leaders, and persistence.
- Several functions (e.g., signature verification, fraud proofs) are stubbed and require implementation for production use.
- The system aims for MEV resistance through deterministic transaction ordering.