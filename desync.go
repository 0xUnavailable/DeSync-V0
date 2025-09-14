package main

// Updated Desync prototype
// - Aligned with DESYNC v3.1 blueprint
// - NOTE: This remains a prototype. For production you must:
//   * Replace the vrf stubs with a real VRF (e.g. libp2p/crypto VRF or EC-VRF)
//   * Integrate with Tendermint ABCI for CheckTx/DeliverTx/Commit lifecycle
//   * Use a proven database (LevelDB/RocksDB) for persistence and atomic state
//   * Harden slashing, staking, and evidence logic
//
// Key improvements in this file:
// - Deterministic ordering uses BlockHeight + IndexInBlock (not local timestamps)
// - computeStateRoot implemented as a Merkle tree over key||value leaves
// - Receipt and BatchResult types added, with a basic VerifyReceipt implementation
// - Persistence uses atomic tempfile + rename for durability
// - VRF replaced with HMAC-SHA256-based deterministic stub (replace with real VRF)
// - Many TODOs call out where production work is required

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
	"github.com/ProtonMail/go-ecvrf/ecvrf"

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
	BatchID      uint64
	BlockHeight  uint64
	Sequence     uint64
	TxHashes     [][]byte
	PrevStateRoot []byte
	WorkerID     uint64
}

type Proof struct {
	Key        []byte
	OldValue   []byte
	NewValue   []byte
	MerkleProof [][]byte // sibling hashes from leaf up to root
}

type BatchResult struct {
	BatchID          uint64
	WorkerID         uint64
	PostStateRoot    []byte
	Proofs           []Proof
	ExecutionSummary struct {
		TxCount     uint32
		SuccessCount uint32
		GasUsed     uint64
	}
	Signature []byte
}

type Receipt struct {
	BatchID       uint64
	WorkerID      uint64
	PostStateRoot []byte
	Signature     []byte
}

type Evidence struct {
	ReporterID       uint64
	OffendingReceipt Receipt
	CounterProof     Proof
	TimestampUnixMs  int64
	Signature        []byte
}

// ---------------------------
// Persistent state
// ---------------------------

type PersistentState struct {
	CurrentEpoch uint64
	NodeID       uint64
	PrivateKey   []byte // ed25519 seed or similar
	Stake        uint64
	StateRoot    []byte
	// other genesis / cluster config omitted for brevity
}

// persist/restore use gob to (atomically) write to disk
var persistMu sync.Mutex

func persistState(path string, s *PersistentState) error {
	persistMu.Lock()
	defer persistMu.Unlock()
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(s); err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()
	return os.Rename(tmp, path)
}

func restoreState(path string) (*PersistentState, error) {
	persistMu.Lock()
	defer persistMu.Unlock()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var s PersistentState
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// ---------------------------
// Execution state (in-memory prototype)
// ---------------------------

var executionState = make(map[string][]byte) // key -> value
var execMu sync.RWMutex

// Helper: deterministic list of keys for Merkle leaves
func sortedKeysMap(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// computeStateRoot: Merkle tree over leaves H(key || value)
// Returns root and map of leaf hash to its index for proof generation.
func computeStateRootFromMap(state map[string][]byte) ([]byte, []byte, map[string]int) {
	// Build leaves
	keys := sortedKeysMap(state)
	leaves := make([][]byte, 0, len(keys))
	indexOf := make(map[string]int)
	for i, k := range keys {
		d := append([]byte(k), state[k]...)
		h := sha256.Sum256(d)
		leaves = append(leaves, h[:])
		indexOf[k] = i
	}
	// If no leaves, return zero root
	if len(leaves) == 0 {
		empty := sha256.Sum256([]byte{})
		return empty[:], nil, indexOf
	}
	// Build tree
	nodes := leaves
	for len(nodes) > 1 {
		var next [][]byte
		for i := 0; i < len(nodes); i += 2 {
			if i+1 == len(nodes) {
				// duplicate last node (simple Merkle padding)
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
	// For convenience also return concatenated leaf hashes blob (not required)
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

// Generate Merkle inclusion proof for a given key, using current executionState snapshot
// Proof is list of sibling hashes from leaf to root.
func generateInclusionProof(key string, state map[string][]byte) ([][]byte, error) {
	keys := sortedKeysMap(state)
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
				right = nodes[j] // duplicate
			}
			// Determine sibling for the tracked index
			if j == i || j+1 == i {
				// sibling is either left or right depending
				if j == i {
					proof = append(proof, right)
				} else {
					proof = append(proof, left)
				}
				// new index in next level
				newIndex := len(next)
				_ = newIndex
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
// VRF stub (HMAC-SHA256 based) - using ecvrf for real VRF
// ---------------------------
// vrfGenerateHash: prove VRF output from seed and private key bytes.
// Returns beta (sha256(proof)) as VRF output, proof, error.
func vrfGenerateHash(seed []byte, privateKeyBytes []byte) ([]byte, []byte, error) {
    sk, err := ecvrf.NewPrivateKey(privateKeyBytes)
    if err != nil {
        return nil, nil, err
    }

    beta, proof, err := sk.Prove(seed)
    if err != nil {
        return nil, nil, err
    }

    // beta is already the VRF output from library
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
	// This is a simplified check: verify each proof that old_value matches prevState and that new_value is set.
	// Then compute a candidate root from applying the changes and compare with r.PostStateRoot.
	// Note: For production, proofs must be full SMT inclusion proofs and verification must be strict.

	// Build a copy of prevState
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
			worker := activeWorkers[(i/ maxBatchSize) % workerCount]
			b := Batch{
				BatchID: seq,
				BlockHeight: ordered[0].BlockHeight,
				Sequence: seq,
				TxHashes: batchTxHashes,
				PrevStateRoot: prevStateRoot,
				WorkerID: worker,
			}
			batches = append(batches, b)
			seq++
			batchTxHashes = nil
		}
	}
	return batches
}

// executeBatchWithDependencies: worker applies transactions deterministically using StateMachine.Apply (provided as interface)
// For prototype, we model StateMachine.Apply as a simple function that sets key->value in executionState map.
type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

type SimpleStateMachine struct{}
func (s *SimpleStateMachine) Apply(cmd []byte) ([]byte, error) {
    parts := bytes.SplitN(cmd, []byte(":"), 2)
    if len(parts) != 2 {
        return nil, fmt.Errorf("invalid cmd format, expected key:value")
    }
    key := string(parts[0])
    value := parts[1]

    execMu.Lock()
    executionState[key] = append([]byte(nil), value...)
    execMu.Unlock()

    return []byte("OK"), nil
}


func executeBatchWithDependencies(b Batch, sm StateMachine, prevState map[string][]byte) (BatchResult, error) {
	start := time.Now()
	var res BatchResult
	res.BatchID = b.BatchID
	res.WorkerID = b.WorkerID

	// For prototype, we simulate executing each tx by applying tx.TxData to state (assuming it's "key:val")
	// and record state changes for proofs.
	changes := make(map[string][]byte)
	txCount := uint32(0)
	successCount := uint32(0)
	var gasUsed uint64 = 0

	for _, th := range b.TxHashes {
		// In a real system we would fetch the full tx by hash; here we assume TxData is reachable.
		// For prototype, assume tx data encodes key:value and is available in a global map (omitted).
		// We'll just simulate a change with a derived key.
		txCount++
		txKey := fmt.Sprintf("batch_%d_tx_%d", b.BatchID, txCount)
		newVal := sha256.Sum256(th)
		changes[txKey] = newVal[:]
		successCount++
		gasUsed += 1
	}

	// Build proofs and compute post state root by applying changes to prevState snapshot
	tmp := make(map[string][]byte)
	for k, v := range prevState {
		tmp[k] = append([]byte(nil), v...)
	}
	for k, v := range changes {
		old := tmp[k]
		proof, _ := generateInclusionProof(k, tmp) // may return error if key missing
		p := Proof{
			Key: []byte(k),
			OldValue: old,
			NewValue: v,
			MerkleProof: proof,
		}
		res.Proofs = append(res.Proofs, p)
		tmp[k] = append([]byte(nil), v...)
	}

	root, _, _ := computeStateRootFromMap(tmp)
	res.PostStateRoot = root
	res.ExecutionSummary.TxCount = txCount
	res.ExecutionSummary.SuccessCount = successCount
	res.ExecutionSummary.GasUsed = gasUsed

	// Signature stub: sign PostStateRoot using node private key if available
	// For prototype we leave Signature empty (production: sign with worker private key)
	res.Signature = []byte{}

	// record duration
	_ = start
	return res, nil
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
// ---------------------------

func main() {
	// Demo: setup state, create txs, order them, create batches, execute, verify receipts
	fmt.Println("DESYNC prototype (updated). Demo run...")

	// persistent state load/save demo
	p := &PersistentState{
		CurrentEpoch: 1,
		NodeID: 1,
		PrivateKey: []byte("demo-private-key-32bytes-length-should-be-replaced"),
		Stake: 1000,
		StateRoot: []byte{},
	}
	_ = persistState("/tmp/desync_state.gob", p)
	restored, err := restoreState("/tmp/desync_state.gob")
	if err != nil {
		fmt.Println("restore error:", err)
	} else {
		fmt.Println("restored epoch:", restored.CurrentEpoch)
	}

	// create fake transactions
	txs := []Transaction{}
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("key%d:%d", i, i*10))
		tx := Transaction{
			TxData: data,
			TxID: computeTxID(data),
			BlockHeight: 100,
			IndexInBlock: uint32(i),
		}
		txs = append(txs, tx)
	}

	ordered := orderTransactionsDeterministic(txs)
	activeWorkers := []uint64{101, 102}
	prevState := map[string][]byte{}
	root, _, _ := computeStateRootFromMap(prevState)
	batches := createSequentialBatches(ordered, activeWorkers, 4, 1, root)
	fmt.Printf("Created %d batches\n", len(batches))
	sm := &SimpleStateMachine{}
	for _, b := range batches {
		br, err := executeBatchWithDependencies(b, sm, prevState)
		if err != nil {
			fmt.Println("execute error:", err)
			continue
		}
		fmt.Printf("Batch %d -> post_root %x\n", b.BatchID, br.PostStateRoot)
		// verify receipt (very basic)
		rec := Receipt{BatchID: br.BatchID, WorkerID: br.WorkerID, PostStateRoot: br.PostStateRoot}
		ok, verr := verifyReceipt(rec, br.Proofs, prevState)
		if ok {
			fmt.Printf("Receipt verified for batch %d\n", b.BatchID)
			// apply changes to prevState for next batch (simulate)
			for _, p := range br.Proofs {
				prevState[string(p.Key)] = append([]byte(nil), p.NewValue...)
			}
		} else {
			fmt.Printf("Receipt verification failed: %v\n", verr)
		}
	}
}

// End of file
