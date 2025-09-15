package main

// DESYNC prototype with LevelDB-backed StateDB persistence.
//
// Key changes in this version:
// - Execution state persisted into LevelDB (StateDB struct).
// - SimpleStateMachine.Apply can write into LevelDB or into an in-memory staging map.
// - executeBatchWithDependencies can run in "staging" mode and return changes instead of writing directly.
// - New helper applyChangesToDB writes a merged set of changes to StateDB in one pass.
// - computeStateRoot and inclusion proof functions can operate over StateDB or an in-memory map.
// - PersistentState still stored as a gob blob under LevelDB key "persistent_state".
// - DB path is configurable (default ./desync_db for demo).
//
// Notes:
// - This prototype still materializes keys into memory for Merkle proofs.
// - The staging approach helps avoid DB write contention by aggregating writes and committing once.

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"

	"github.com/ProtonMail/go-ecvrf/ecvrf"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// ---------------------------
// Data structures (aligned with spec v3.1)
// ---------------------------

type Transaction struct {
	TxID         []byte // sha256(tx_data)
	TxData       []byte
	Signature    []byte
	PubKey       []byte
	Nonce        uint64
	GasPrice     uint64
	BlockHeight  uint64 // deterministic ordering source (set by proposer/ABCI)
	IndexInBlock uint32 // deterministic ordering source (set by proposer/ABCI)
}

type Batch struct {
	BatchID       uint64
	BlockHeight   uint64
	Sequence      uint64
	TxHashes      [][]byte
	PrevStateRoot []byte
	WorkerID      uint64
}

type Proof struct {
	Key         []byte
	OldValue    []byte
	NewValue    []byte
	MerkleProof [][]byte // sibling hashes from leaf up to root
}

type BatchResult struct {
	BatchID       uint64
	WorkerID      uint64
	PostStateRoot []byte
	Proofs        []Proof
	ExecutionSummary struct {
		TxCount      uint32
		SuccessCount uint32
		GasUsed      uint64
	}
	Signature []byte
	// Changes collected when running in staging mode
	Changes map[string][]byte
}

type Receipt struct {
	BatchID       uint64
	WorkerID      uint64
	PostStateRoot []byte
	Signature     []byte
}

type Evidence struct {
	ReporterID      uint64
	OffendingReceipt Receipt
	CounterProof    Proof
	TimestampUnixMs int64
	Signature       []byte
}

// ---------------------------
// Persistent state (metadata) stored in LevelDB under key "persistent_state"
// ---------------------------

type PersistentState struct {
	CurrentEpoch uint64
	NodeID       uint64
	PrivateKey   []byte // ed25519 seed or similar (for demo)
	Stake        uint64
	StateRoot    []byte
	// other genesis / cluster config omitted for brevity
}

// ---------------------------
// LevelDB-backed StateDB abstraction
// ---------------------------

type StateDB struct {
	db   *leveldb.DB
	path string
	mu   sync.RWMutex // guards db operations
}

// NewStateDB opens (or creates) a LevelDB database at path.
func NewStateDB(path string) (*StateDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &StateDB{db: db, path: path}, nil
}

func (s *StateDB) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// Put stores a string key and value (internal helper).
func (s *StateDB) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Put([]byte(key), value, nil)
}

// Get reads value for a string key (internal helper).
// Returns nil,nil if not found.
func (s *StateDB) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return v, err
}

// PutState stores an arbitrary key (as bytes) -> value.
// Provided for backward compatibility with earlier code.
func (s *StateDB) PutState(key []byte, value []byte) error {
	ks := string(key)
	return s.Put(ks, value)
}

// GetState returns value for a byte key or nil if missing.
func (s *StateDB) GetState(key []byte) ([]byte, error) {
	ks := string(key)
	return s.Get(ks)
}

// DeleteState removes a key (byte).
func (s *StateDB) DeleteState(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Delete([]byte(string(key)), nil)
}

// IterateAll returns a map[string][]byte containing all keys/values in the DB.
// For prototype usage only: materializes entire DB into memory.
func (s *StateDB) IterateAll(prefix string) (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]byte)

	var iter iterator.Iterator
	if prefix == "" {
		iter = s.db.NewIterator(nil, nil)
	} else {
		iter = s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	}
	defer iter.Release()

	for iter.Next() {
		k := string(iter.Key())
		v := iter.Value()
		// copy bytes
		c := make([]byte, len(v))
		copy(c, v)
		result[k] = c
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}
	return result, nil
}

// Commit stores a state root for a given block (under key block_<height>_root)
func (s *StateDB) Commit(blockHeight uint64, root []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("block_%d_root", blockHeight)
	return s.db.Put([]byte(key), root, nil)
}

// GetRoot retrieves the committed state root for a block (may return ErrNotFound)
func (s *StateDB) GetRoot(blockHeight uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("block_%d_root", blockHeight)
	v, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return v, err
}

// DumpToJSON exports the entire DB into a JSON file.
// Each key/value is stored as string -> hex (safe for binary data).
func (s *StateDB) DumpToJSON(filename string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string)

	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		k := string(iter.Key())
		v := iter.Value()
		result[k] = hex.EncodeToString(v)
	}
	if err := iter.Error(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}



// Persist PersistentState as a gob-encoded blob under key "persistent_state".
func (s *StateDB) SavePersistentState(ps *PersistentState) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(ps); err != nil {
		return err
	}
	return s.Put("persistent_state", buf.Bytes())
}

func (s *StateDB) LoadPersistentState() (*PersistentState, error) {
	data, err := s.Get("persistent_state")
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("persistent_state not found")
	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	var ps PersistentState
	if err := dec.Decode(&ps); err != nil {
		return nil, err
	}
	return &ps, nil
}


// ---------------------------
// Execution state uses StateDB rather than global map
// ---------------------------

var stateDB *StateDB // global DB handle used by functions below

// Helper: deterministic list of keys for Merkle leaves (from in-memory map)
// We'll materialize from DB for prototype proofs.
func sortedKeysFromMap(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// computeStateRootFromMap: same Merkle algorithm as before
func computeStateRootFromMap(state map[string][]byte) ([]byte, []byte, map[string]int) {
	keys := sortedKeysFromMap(state)
	leaves := make([][]byte, 0, len(keys))
	indexOf := make(map[string]int)
	for i, k := range keys {
		d := append([]byte(k), state[k]...)
		h := sha256.Sum256(d)
		leaves = append(leaves, h[:])
		indexOf[k] = i
	}
	if len(leaves) == 0 {
		empty := sha256.Sum256([]byte{})
		return empty[:], nil, indexOf
	}
	nodes := leaves
	for len(nodes) > 1 {
		var next [][]byte
		for i := 0; i < len(nodes); i += 2 {
			if i+1 == len(nodes) {
				h := sha256.Sum256(append(nodes[i], nodes[i]...))
				next = append(next, h[:])
			} else {
				concat := append(nodes[i], nodes[i+1]...)
				h := sha256.Sum256(concat)
				next = append(next, h[:])
			}
		}
		nodes = next
	}
	root := nodes[0]
	leavesConcat := bytesConcat(leaves)
	return root, leavesConcat, indexOf
}

func bytesConcat(arr [][]byte) []byte {
	var out []byte
	for _, a := range arr {
		out = append(out, a...)
	}
	return out
}

// computeStateRootFromDB materializes DB into a map (prefix ""), then computes root.
// For production you must use an incremental tree instead.
func computeStateRootFromDB(db *StateDB) ([]byte, error) {
	m, err := db.IterateAll("")
	if err != nil {
		return nil, err
	}
	// Exclude persistent_state key from state
	delete(m, "persistent_state")
	root, _, _ := computeStateRootFromMap(m)
	return root, nil
}

// generateInclusionProofFromDB generates inclusion proof for a key by materializing DB.
// Returns proof (list of sibling hashes).
func generateInclusionProofFromDB(db *StateDB, key string) ([][]byte, error) {
	m, err := db.IterateAll("")
	if err != nil {
		return nil, err
	}
	delete(m, "persistent_state")
	return generateInclusionProof(key, m)
}

// generateInclusionProof given a materialized map (same as before)
func generateInclusionProof(key string, state map[string][]byte) ([][]byte, error) {
	keys := sortedKeysFromMap(state)
	index := -1
	for i, k := range keys {
		if k == key {
			index = i
			break
		}
	}
	if index == -1 {
		return nil, errors.New("key-not-found")
	}
	// Build leaves
	leaves := make([][]byte, 0, len(keys))
	for _, k := range keys {
		d := append([]byte(k), state[k]...)
		h := sha256.Sum256(d)
		leaves = append(leaves, h[:])
	}
	// Build proof
	proof := make([][]byte, 0)
	i := index
	nodes := leaves
	for len(nodes) > 1 {
		var next [][]byte
		for j := 0; j < len(nodes); j += 2 {
			var left, right []byte
			left = nodes[j]
			if j+1 < len(nodes) {
				right = nodes[j+1]
			} else {
				right = nodes[j]
			}
			if j == i || j+1 == i {
				if j == i {
					proof = append(proof, right)
				} else {
					proof = append(proof, left)
				}
			}
			concat := append(left, right...)
			h := sha256.Sum256(concat)
			next = append(next, h[:])
		}
		i = i / 2
		nodes = next
	}
	return proof, nil
}

// verifyInclusionProof verifies that leafHash combined with proof yields root using pairing strategy
func verifyInclusionProof(leafHash []byte, proof [][]byte, index int) []byte {
	hash := leafHash
	i := index
	for _, sibling := range proof {
		if i%2 == 0 {
			concat := append(hash, sibling...)
			h := sha256.Sum256(concat)
			hash = h[:]
		} else {
			concat := append(sibling, hash...)
			h := sha256.Sum256(concat)
			hash = h[:]
		}
		i = i / 2
	}
	return hash
}

// ---------------------------
// VRF: go-ecvrf (EC-VRF Ed25519) implementation
// ---------------------------

// vrfGenerateHash: prove VRF output from seed and private key bytes.
// Returns beta (library vrf output), proof, error.
func vrfGenerateHash(seed []byte, privateKeyBytes []byte) ([]byte, []byte, error) {
	sk, err := ecvrf.NewPrivateKey(privateKeyBytes)
	if err != nil {
		return nil, nil, err
	}

	beta, proof, err := sk.Prove(seed)
	if err != nil {
		return nil, nil, err
	}

	// beta is the VRF output from the library
	return beta, proof, nil
}

func vrfVerifyHash(hash []byte, proof []byte, publicKeyBytes []byte, seed []byte) (bool, error) {
	pk, err := ecvrf.NewPublicKey(publicKeyBytes)
	if err != nil {
		return false, err
	}

	ok, beta2, err := pk.Verify(seed, proof)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	// Ensure the verified output matches the expected hash
	return bytes.Equal(beta2, hash), nil
}

// ---------------------------
// Receipt verification (basic)
// ---------------------------

func verifyReceipt(r Receipt, proofs []Proof, prevState map[string][]byte) (bool, error) {
	// recompute post state root from prevState + proofs
	tmp := make(map[string][]byte)
	for k, v := range prevState {
		tmp[k] = append([]byte(nil), v...)
	}
	// Apply proofs as diffs
	for _, p := range proofs {
		k := string(p.Key)
		// verify old value matches
		old := tmp[k]
		if !byteSlicesEqual(old, p.OldValue) {
			return false, fmt.Errorf("old value mismatch for key %x", p.Key)
		}
		// apply new
		tmp[k] = append([]byte(nil), p.NewValue...)
	}
	// compute root
	root, _, _ := computeStateRootFromMap(tmp)
	if !byteSlicesEqual(root, r.PostStateRoot) {
		return false, fmt.Errorf("post state root mismatch")
	}
	// signature verify - omitted here (requires worker pubkey registry)
	return true, nil
}

func byteSlicesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------
// Deterministic ordering helper
// ---------------------------
func txLess(a, b Transaction) bool {
	// First by BlockHeight, then IndexInBlock, then TxID lexicographic
	if a.BlockHeight != b.BlockHeight {
		return a.BlockHeight < b.BlockHeight
	}
	if a.IndexInBlock != b.IndexInBlock {
		return a.IndexInBlock < b.IndexInBlock
	}
	return hex.EncodeToString(a.TxID) < hex.EncodeToString(b.TxID)
}

// orderTransactionsDeterministic: sort by blockHeight,index,txid
func orderTransactionsDeterministic(txs []Transaction) []Transaction {
	sort.SliceStable(txs, func(i, j int) bool {
		return txLess(txs[i], txs[j])
	})
	return txs
}

// ---------------------------
// Batch creation and execution (leader & worker prototypes)
// ---------------------------

func createSequentialBatches(ordered []Transaction, activeWorkers []uint64, maxBatchSize int, startSequence uint64, prevStateRoot []byte) []Batch {
	batches := []Batch{}
	if len(activeWorkers) == 0 {
		return batches
	}
	var batchTxHashes [][]byte
	seq := startSequence
	workerCount := len(activeWorkers)
	for i, tx := range ordered {
		batchTxHashes = append(batchTxHashes, tx.TxID)
		if len(batchTxHashes) >= maxBatchSize || i == len(ordered)-1 {
			worker := activeWorkers[(i/maxBatchSize)%workerCount]
			b := Batch{
				BatchID:       seq,
				BlockHeight:   ordered[0].BlockHeight,
				Sequence:      seq,
				TxHashes:      batchTxHashes,
				PrevStateRoot: prevStateRoot,
				WorkerID:      worker,
			}
			batches = append(batches, b)
			seq++
			batchTxHashes = nil
		}
	}
	return batches
}

// executeBatchWithDependencies: worker applies transactions deterministically using StateMachine.Apply (provided as interface)
// For prototype, executeBatchWithDependencies can run in one of two modes:
//  - staging mode: sm has an in-memory staging map and changes are collected and returned (no DB writes)
//  - live mode: sm writes directly to the provided StateDB
// This function returns BatchResult and, when run in staging mode, a Changes map inside the BatchResult.

// ---------------------------
// StateMachine Abstractions
// ---------------------------

// StateMachine defines the contract for applying transactions.
type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

// SimpleStateMachine can write into LevelDB or into an in-memory staging map.
type SimpleStateMachine struct {
	DB      *StateDB
	Staging map[string][]byte // if non-nil, writes go here
}

func NewSimpleStateMachine(db *StateDB) *SimpleStateMachine {
	return &SimpleStateMachine{DB: db}
}

func NewStagingStateMachine(staging map[string][]byte) *SimpleStateMachine {
	return &SimpleStateMachine{Staging: staging}
}

func (s *SimpleStateMachine) Apply(cmd []byte) ([]byte, error) {
	parts := bytes.SplitN(cmd, []byte(":"), 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cmd format, expected key:value")
	}
	key := string(parts[0])
	value := parts[1]

	// staging mode
	if s.Staging != nil {
		v := make([]byte, len(value))
		copy(v, value)
		s.Staging[key] = v
		return []byte("OK"), nil
	}

	// live DB mode
	if s.DB == nil {
		return nil, fmt.Errorf("stateDB not initialized")
	}
	return []byte("OK"), s.DB.Put(key, append([]byte(nil), value...))
}

// ---------------------------
// Batch execution
// ---------------------------

func executeBatchWithDependencies(db *StateDB, b Batch, sm StateMachine, prevState map[string][]byte) (BatchResult, error) {
	var res BatchResult
	res.BatchID = b.BatchID
	res.WorkerID = b.WorkerID

	changes := make(map[string][]byte)
	var txCount, successCount uint32
	var gasUsed uint64

	// detect staging mode
	stagingMode := false
	if ssm, ok := sm.(*SimpleStateMachine); ok && ssm.Staging != nil {
		stagingMode = true
	}

	for i, th := range b.TxHashes {
		txCount++
		txKey := fmt.Sprintf("batch_%d_tx_%d", b.BatchID, i)
		newVal := sha256.Sum256(th)

		changes[txKey] = newVal[:]
		successCount++
		gasUsed++

		cmd := []byte(fmt.Sprintf("%s:%x", txKey, newVal[:]))
		if _, err := sm.Apply(cmd); err != nil {
			return res, fmt.Errorf("apply failed: %w", err)
		}
	}

	// Build proofs: in staging mode, materialize DB + staging merged snapshot
	var m map[string][]byte
	if stagingMode {
		// If a DB was provided, snapshot it; otherwise treat DB snapshot as empty.
		var dbState map[string][]byte
		if db != nil {
			var err error
			dbState, err = db.IterateAll("")
			if err != nil {
				return res, err
			}
			delete(dbState, "persistent_state")
		} else {
			// no DB available in this context (e.g., tests). Start from empty state.
			dbState = make(map[string][]byte)
		}

		// merge staging on top
		ssm := sm.(*SimpleStateMachine)
		for k, v := range ssm.Staging {
			dbState[k] = v
		}
		m = dbState
	} else {
		// live mode requires a real DB
		if db == nil {
			return res, fmt.Errorf("live execution requires a non-nil StateDB")
		}
		var err error
		m, err = db.IterateAll("")
		if err != nil {
			return res, err
		}
		delete(m, "persistent_state")
	}

	for k, v := range changes {
		old := m[k]
		proof, _ := generateInclusionProof(k, m)
		res.Proofs = append(res.Proofs, Proof{
			Key:         []byte(k),
			OldValue:    old,
			NewValue:    v,
			MerkleProof: proof,
		})
		m[k] = v
	}

	root, _, _ := computeStateRootFromMap(m)
	res.PostStateRoot = root
	res.ExecutionSummary.TxCount = txCount
	res.ExecutionSummary.SuccessCount = successCount
	res.ExecutionSummary.GasUsed = gasUsed
	res.Changes = changes

	return res, nil
}

// Deterministic merge: always sort keys before committing
func applyChangesToDB(db *StateDB, changes map[string][]byte) error {
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := db.Put(k, changes[k]); err != nil {
			return err
		}
	}
	return nil
}


// ---------------------------
// Utilities
// ---------------------------
func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// simple helper to compute tx id
func computeTxID(txData []byte) []byte {
	h := sha256.Sum256(txData)
	return h[:]
}

// ---------------------------
// Small demo of usage (not networked) - demonstrates create batches and compute roots
// Using staging: workers produce changes, then we merge and write them in one pass.
// ---------------------------

func main() {
	fmt.Println("DESYNC prototype (staging writes). Demo run...")

	// DB path
	dbPath := "./desync_db"

	// Open / create DB
	var err error
	stateDB, err = NewStateDB(dbPath)
	if err != nil {
		log.Fatalf("failed to open state DB: %v", err)
	}
	defer stateDB.Close()

	// Load or initialize persistent state
	ps, err := stateDB.LoadPersistentState()
	if err != nil {
		ps = &PersistentState{
			CurrentEpoch: 1,
			NodeID:       1,
			PrivateKey:   []byte("demo-private-key-32bytes-length-should-be-replaced"),
			Stake:        1000,
			StateRoot:    []byte{},
		}
		if err := stateDB.SavePersistentState(ps); err != nil {
			log.Fatalf("failed to save persistent_state: %v", err)
		}
	}
	fmt.Println("restored epoch:", ps.CurrentEpoch)

	// Create fake transactions
	txs := []Transaction{}
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("key%d:%d", i, i*10))
		tx := Transaction{
			TxData:       data,
			TxID:         computeTxID(data),
			BlockHeight:  100,
			IndexInBlock: uint32(i),
		}
		txs = append(txs, tx)
	}
	ordered := orderTransactionsDeterministic(txs)

	// Compute previous state root
	prevRoot, err := computeStateRootFromDB(stateDB)
	if err != nil {
		prevRoot = []byte{} // if DB empty
	}

	// Create batches
	activeWorkers := []uint64{101, 102}
	batches := createSequentialBatches(ordered, activeWorkers, 4, 1, prevRoot)
	fmt.Printf("Created %d batches\n", len(batches))

	// --- STAGING EXECUTION ---
	workerChanges := make([]map[string][]byte, len(batches))
	var wg sync.WaitGroup

	for i, b := range batches {
		wg.Add(1)
		staging := make(map[string][]byte)

		go func(idx int, batch Batch, st map[string][]byte) {
			defer wg.Done()

			// worker executes batch in staging mode
			sm := NewStagingStateMachine(st)
			br, err := executeBatchWithDependencies(stateDB, batch, sm, nil)
			if err != nil {
				fmt.Printf("worker %d execute error: %v\n", batch.WorkerID, err)
				return
			}
			workerChanges[idx] = br.Changes
		}(i, b, staging)
	}
	wg.Wait()

	// Merge all staged changes deterministically
	merged := make(map[string][]byte)
	for _, ch := range workerChanges {
		if ch == nil {
			continue
		}
		for k, v := range ch {
			merged[k] = v // last writer wins if conflict
		}
	}

	// Apply merged changes to DB in one pass
	if err := applyChangesToDB(stateDB, merged); err != nil {
		log.Fatalf("applyChangesToDB failed: %v", err)
	}

	// Compute and persist final root
	root, _ := computeStateRootFromDB(stateDB)
	fmt.Printf("Final root after merged commit: %x\n", root)

	ps.StateRoot = root
	_ = stateDB.SavePersistentState(ps)
}
