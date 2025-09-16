package main

// DESYNC prototype with LevelDB-backed StateDB persistence.
//
// Key changes in this version:
// - Execution state persisted into LevelDB (StateDB struct).
// - SimpleStateMachine.Apply can write into LevelDB or into an in-memory staging map.
// - Workers stage changes in-memory, then the leader merges ordered results and commits in one LevelDB batch.
// - Keys for committed state are built deterministically from transaction IndexInBlock (tx_<index>).
// - computeStateRoot and inclusion proof functions still materialize DB (prototype).
//
// Notes:
// - This prototype materializes DB into memory for Merkle proofs (not scalable; use IAVL/IAVL+Merkle in prod).
// - Deep copies are used when returning staged data to avoid aliasing across goroutines.

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/ProtonMail/go-ecvrf/ecvrf"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// ---------------------------
// Data structures
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
	TxIndices     []uint32 // preserves original IndexInBlock for each tx in TxHashes
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
	Changes   map[string][]byte
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

type PersistentState struct {
	CurrentEpoch uint64
	NodeID       uint64
	PrivateKey   []byte
	Stake        uint64
	StateRoot    []byte
}

// ---------------------------
// LevelDB-backed StateDB
// ---------------------------

type StateDB struct {
	db   *leveldb.DB
	path string
	mu   sync.RWMutex
}

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

func (s *StateDB) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Put([]byte(key), value, nil)
}

func (s *StateDB) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return v, err
}

func (s *StateDB) PutState(key []byte, value []byte) error {
	ks := string(key)
	return s.Put(ks, value)
}

func (s *StateDB) GetState(key []byte) ([]byte, error) {
	ks := string(key)
	return s.Get(ks)
}

func (s *StateDB) DeleteState(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Delete([]byte(string(key)), nil)
}

// IterateAll materializes DB into memory. Prototype only.
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
		c := make([]byte, len(v))
		copy(c, v)
		result[k] = c
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *StateDB) Commit(blockHeight uint64, root []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("block_%d_root", blockHeight)
	return s.db.Put([]byte(key), root, nil)
}

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

// DumpToJSON now deterministic: sorted keys -> ordered array of {key,value}
func (s *StateDB) DumpToJSON(filename string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	kvs := make(map[string]string)
	keys := make([]string, 0)
	for iter.Next() {
		k := string(iter.Key())
		v := iter.Value()
		kvs[k] = hex.EncodeToString(v)
		keys = append(keys, k)
	}
	if err := iter.Error(); err != nil {
		return err
	}
	sort.Strings(keys)

	ordered := make([]map[string]string, 0, len(keys))
	for _, k := range keys {
		ordered = append(ordered, map[string]string{"key": k, "value": kvs[k]})
	}

	data, err := json.MarshalIndent(ordered, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

func (s *StateDB) NewBatch() *leveldb.Batch {
	return new(leveldb.Batch)
}

func (s *StateDB) WriteBatch(b *leveldb.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Write(b, nil)
}

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
// Execution helpers (Merkle prototype)
// ---------------------------

var stateDB *StateDB

func sortedKeysFromMap(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

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

func computeStateRootFromDB(db *StateDB) ([]byte, error) {
	m, err := db.IterateAll("")
	if err != nil {
		return nil, err
	}
	delete(m, "persistent_state")
	root, _, _ := computeStateRootFromMap(m)
	return root, nil
}

func generateInclusionProofFromDB(db *StateDB, key string) ([][]byte, error) {
	m, err := db.IterateAll("")
	if err != nil {
		return nil, err
	}
	delete(m, "persistent_state")
	return generateInclusionProof(key, m)
}

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
	leaves := make([][]byte, 0, len(keys))
	for _, k := range keys {
		d := append([]byte(k), state[k]...)
		h := sha256.Sum256(d)
		leaves = append(leaves, h[:])
	}
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
// VRF (kept as helper)
// ---------------------------

func vrfGenerateHash(seed []byte, privateKeyBytes []byte) ([]byte, []byte, error) {
	sk, err := ecvrf.NewPrivateKey(privateKeyBytes)
	if err != nil {
		return nil, nil, err
	}
	beta, proof, err := sk.Prove(seed)
	if err != nil {
		return nil, nil, err
	}
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
	return bytes.Equal(beta2, hash), nil
}

// ---------------------------
// Receipt verification
// ---------------------------

func verifyReceipt(r Receipt, proofs []Proof, prevState map[string][]byte) (bool, error) {
	// sort proofs deterministically by key to defend against out-of-order proofs
	sort.Slice(proofs, func(i, j int) bool {
		return string(proofs[i].Key) < string(proofs[j].Key)
	})

	tmp := make(map[string][]byte)
	for k, v := range prevState {
		tmp[k] = append([]byte(nil), v...)
	}
	for _, p := range proofs {
		k := string(p.Key)
		old := tmp[k]
		if !byteSlicesEqual(old, p.OldValue) {
			return false, fmt.Errorf("old value mismatch for key %x", p.Key)
		}
		tmp[k] = append([]byte(nil), p.NewValue...)
	}
	root, _, _ := computeStateRootFromMap(tmp)
	if !byteSlicesEqual(root, r.PostStateRoot) {
		return false, fmt.Errorf("post state root mismatch")
	}
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
	if a.BlockHeight != b.BlockHeight {
		return a.BlockHeight < b.BlockHeight
	}
	if a.IndexInBlock != b.IndexInBlock {
		return a.IndexInBlock < b.IndexInBlock
	}
	return hex.EncodeToString(a.TxID) < hex.EncodeToString(b.TxID)
}

func orderTransactionsDeterministic(txs []Transaction) []Transaction {
	sort.SliceStable(txs, func(i, j int) bool {
		return txLess(txs[i], txs[j])
	})
	return txs
}

// ---------------------------
// Batch creation and execution
// ---------------------------

func createSequentialBatches(ordered []Transaction, activeWorkers []uint64, maxBatchSize int, startSequence uint64, prevStateRoot []byte) []Batch {
	batches := []Batch{}
	if len(activeWorkers) == 0 {
		return batches
	}
	var batchTxHashes [][]byte
	var batchTxIndices []uint32
	seq := startSequence
	workerCount := len(activeWorkers)
	for i, tx := range ordered {
		batchTxHashes = append(batchTxHashes, tx.TxID)
		batchTxIndices = append(batchTxIndices, tx.IndexInBlock)
		if len(batchTxHashes) >= maxBatchSize || i == len(ordered)-1 {
			worker := activeWorkers[(i/maxBatchSize)%workerCount]
			b := Batch{
				BatchID:       seq,
				BlockHeight:   ordered[0].BlockHeight,
				Sequence:      seq,
				TxHashes:      batchTxHashes,
				TxIndices:     batchTxIndices,
				PrevStateRoot: prevStateRoot,
				WorkerID:      worker,
			}
			batches = append(batches, b)
			seq++
			batchTxHashes = nil
			batchTxIndices = nil
		}
	}
	return batches
}

// ---------------------------
// State machine
// ---------------------------

type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

type SimpleStateMachine struct {
	DB      *StateDB
	Staging map[string][]byte
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

	if s.Staging != nil {
		v := make([]byte, len(value))
		copy(v, value)
		s.Staging[key] = v
		return []byte("OK"), nil
	}

	if s.DB == nil {
		return nil, fmt.Errorf("stateDB not initialized")
	}
	return []byte("OK"), s.DB.Put(key, append([]byte(nil), value...))
}

// ---------------------------
// Batched / staged execution
// ---------------------------

// Execute a batch entirely in staging mode; keys are tx_<index>
func executeBatchStaged(b Batch, sm *SimpleStateMachine) (map[string][]byte, error) {
	// Build changes map and apply via StateMachine (staging)
	for i, th := range b.TxHashes {
		idx := uint32(0)
		if i < len(b.TxIndices) {
			idx = b.TxIndices[i]
		}
		txKey := fmt.Sprintf("tx_%d", idx)
		newVal := sha256.Sum256(th)
		cmd := []byte(fmt.Sprintf("%s:%x", txKey, newVal[:]))
		if _, err := sm.Apply(cmd); err != nil {
			return nil, fmt.Errorf("apply failed: %w", err)
		}
	}

	// Copy staging out to avoid aliasing
	if sm.Staging == nil {
		return map[string][]byte{}, nil
	}
	out := make(map[string][]byte, len(sm.Staging))
	// iterate staging deterministically when copying (not strictly required, but explicit)
	keys := sortedKeysFromMap(sm.Staging)
	for _, k := range keys {
		v := sm.Staging[k]
		c := make([]byte, len(v))
		copy(c, v)
		out[k] = c
	}
	return out, nil
}

// Generic (backwards-compatible) function that can also produce proofs.
func executeBatchWithDependencies(db *StateDB, b Batch, sm StateMachine, prevState map[string][]byte) (BatchResult, error) {
	var res BatchResult
	res.BatchID = b.BatchID
	res.WorkerID = b.WorkerID
	res.Changes = make(map[string][]byte)

	changes := make(map[string][]byte)
	var txCount, successCount uint32
	var gasUsed uint64

	stagingMode := false
	if ssm, ok := sm.(*SimpleStateMachine); ok && ssm.Staging != nil {
		stagingMode = true
	}

	for i, th := range b.TxHashes {
		txCount++
		idx := uint32(0)
		if i < len(b.TxIndices) {
			idx = b.TxIndices[i]
		}
		txKey := fmt.Sprintf("tx_%d", idx)
		newVal := sha256.Sum256(th)
		changes[txKey] = newVal[:]
		successCount++
		gasUsed++

		cmd := []byte(fmt.Sprintf("%s:%x", txKey, newVal[:]))
		if _, err := sm.Apply(cmd); err != nil {
			return res, fmt.Errorf("apply failed: %w", err)
		}
	}

	var m map[string][]byte
	if stagingMode {
		var dbState map[string][]byte
		if db != nil {
			var err error
			dbState, err = db.IterateAll("")
			if err != nil {
				return res, err
			}
			delete(dbState, "persistent_state")
		} else {
			dbState = make(map[string][]byte)
		}
		// merge staging deterministically by sorted keys
		ssm := sm.(*SimpleStateMachine)
		if len(ssm.Staging) > 0 {
			stKeys := sortedKeysFromMap(ssm.Staging)
			for _, k := range stKeys {
				// copy to avoid aliasing
				v := ssm.Staging[k]
				c := make([]byte, len(v))
				copy(c, v)
				dbState[k] = c
			}
		}
		m = dbState
	} else {
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

	// build proofs and apply changes deterministically using sorted keys of 'changes'
	changeKeys := sortedKeysFromMap(changes)
	for _, k := range changeKeys {
		v := changes[k]
		old := m[k]
		proof, _ := generateInclusionProof(k, m)
		res.Proofs = append(res.Proofs, Proof{
			Key:         []byte(k),
			OldValue:    old,
			NewValue:    v,
			MerkleProof: proof,
		})
		// apply change
		m[k] = v
		// record change deterministically in res.Changes map (map iteration still nondet, but we only use map for storage)
		res.Changes[k] = v
	}

	root, _, _ := computeStateRootFromMap(m)
	res.PostStateRoot = root
	res.ExecutionSummary.TxCount = txCount
	res.ExecutionSummary.SuccessCount = successCount
	res.ExecutionSummary.GasUsed = gasUsed

	return res, nil
}

// ---------------------------
// Ordered worker pipeline
// ---------------------------

type KVResult struct {
	Index uint32
	Key   string
	Value []byte
}

var stagingPool = sync.Pool{
	New: func() any { return make(map[string][]byte, 128) },
}

// Single threaded execution
func workerExecute(batch Batch) []KVResult {
	staging := stagingPool.Get().(map[string][]byte)
	for k := range staging {
		delete(staging, k)
	}

	sm := NewStagingStateMachine(staging)
	out, err := executeBatchStaged(batch, sm)
	if err != nil {
		fmt.Printf("worker %d execute error: %v\n", batch.WorkerID, err)
		out = make(map[string][]byte)
	}

	results := make([]KVResult, 0, len(batch.TxHashes))
	for i, idx := range batch.TxIndices {
		key := fmt.Sprintf("tx_%d", idx)
		if v, ok := out[key]; ok {
			c := make([]byte, len(v))
			copy(c, v)
			results = append(results, KVResult{
				Index: idx,
				Key:   key,
				Value: c,
			})
		} else {
			// If no write produced for this key, skip (shouldn't normally happen).
			_ = i
		}
	}

	stagingPool.Put(staging)
	return results
}

// Multithreaded execution
func intraNodeExecute(batch Batch) []KVResult {
	numCores := runtime.NumCPU()
	chunkSize := (len(batch.TxHashes) + numCores - 1) / numCores
	resultsChan := make(chan []KVResult, numCores)
	var wg sync.WaitGroup

	for c := 0; c < numCores; c++ {
		start := c * chunkSize
		end := min((c+1)*chunkSize, len(batch.TxHashes))
		if start >= end {
			break
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			subBatch := Batch{
				BatchID:     batch.BatchID,
				BlockHeight: batch.BlockHeight,
				Sequence:    batch.Sequence,
				TxHashes:    batch.TxHashes[start:end],
				TxIndices:   batch.TxIndices[start:end],
				WorkerID:    batch.WorkerID,
			}
			staging := make(map[string][]byte)
			sm := NewStagingStateMachine(staging)
			out, _ := executeBatchStaged(subBatch, sm)

			localResults := []KVResult{}
			for _, idx := range subBatch.TxIndices {
				key := fmt.Sprintf("tx_%d", idx)
				if v, ok := out[key]; ok {
					localResults = append(localResults, KVResult{
						Index: idx, Key: key, Value: v,
					})
				}
			}
			resultsChan <- localResults
		}(start, end)
	}

	wg.Wait()
	close(resultsChan)

	// Merge all local results
	var allResults []KVResult
	for res := range resultsChan {
		allResults = append(allResults, res...)
	}

	// Sort deterministically by IndexInBlock
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Index < allResults[j].Index
	})
	return allResults
}

func mergeResultsOrdered(workerResults [][]KVResult) []KVResult {
	total := 0
	for _, wr := range workerResults {
		total += len(wr)
	}
	merged := make([]KVResult, 0, total)
	for _, wr := range workerResults {
		merged = append(merged, wr...)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Index < merged[j].Index
	})
	return merged
}

func applyResultsOrdered(db *StateDB, results []KVResult) error {
	// Ensure deterministic key order (lexicographic)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	b := db.NewBatch()
	for _, kv := range results {
		b.Put([]byte(kv.Key), kv.Value)
	}
	return db.WriteBatch(b)
}

// ---------------------------
// Utilities
// ---------------------------

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func computeTxID(txData []byte) []byte {
	h := sha256.Sum256(txData)
	return h[:]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------
// Demo main
// ---------------------------

func main() {
	fmt.Println("DESYNC prototype (staging writes). Demo run...")

	dbPath := "./desync_db"

	var err error
	stateDB, err = NewStateDB(dbPath)
	if err != nil {
		log.Fatalf("failed to open state DB: %v", err)
	}
	defer stateDB.Close()

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

	// create fake transactions in received order (IndexInBlock preserves order)
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

	prevRoot, err := computeStateRootFromDB(stateDB)
	if err != nil {
		prevRoot = []byte{}
	}

	activeWorkers := []uint64{101, 102}
	batches := createSequentialBatches(ordered, activeWorkers, 4, 1, prevRoot)
	fmt.Printf("Created %d batches\n", len(batches))

	workerResults := make([][]KVResult, len(batches))
	var wg sync.WaitGroup
	start := time.Now()

	for i, b := range batches {
		wg.Add(1)
		go func(idx int, batch Batch) {
			defer wg.Done()
			workerResults[idx] = workerExecute(batch) // check out intraNodeExecute for multithreaded execution (replace)
		}(i, b)
	}
	wg.Wait()

	merged := mergeResultsOrdered(workerResults)

	if err := applyResultsOrdered(stateDB, merged); err != nil {
		log.Fatalf("applyResultsOrdered failed: %v", err)
	}

	root, _ := computeStateRootFromDB(stateDB)
	elapsed := time.Since(start)
	fmt.Printf("Final root after merged commit: %x (elapsed %s)\n", root, elapsed)

	ps.StateRoot = root
	_ = stateDB.SavePersistentState(ps)
}
