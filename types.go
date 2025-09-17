package desync

import "encoding/json"

// ---------------------------
// Core Types
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

// ---------------------------
// Proofs
// ---------------------------

// KeyValueProof proves inclusion of a single key/value update against a Merkle root.
type KeyValueProof struct {
	Key         []byte   `json:"key"`
	Value       []byte   `json:"value"`
	MerkleProof [][]byte `json:"merkle_proof"` // sibling hashes from leaf up to root
	Root        []byte   `json:"root"`         // root this proof should verify against
}

// ---------------------------
// Batch & Block Results


// BatchResult is produced after executing a batch but before
// finalization into a receipt. It captures changes, proofs, and summary.
type BatchResult struct {
	BatchID       uint64          `json:"batch_id"`
	WorkerID      uint64          `json:"worker_id"`
	PostStateRoot []byte          `json:"post_state_root"`
	Proofs        []KeyValueProof `json:"proofs"`
	ExecutionSummary struct {
		TxCount      uint32 `json:"tx_count"`
		SuccessCount uint32 `json:"success_count"`
		GasUsed      uint64 `json:"gas_used"`
	} `json:"execution_summary"`
	Signature       []byte            `json:"signature"`
	Changes         map[string][]byte `json:"changes"`
	TimestampUnixMs int64             `json:"timestamp_unix_ms"`
}


// BatchReceipt is the canonical receipt exported for external verification.
type BatchReceipt struct {
	BatchID          uint64           `json:"batch_id"`
	BlockHeight      uint64           `json:"block_height"`
	WorkerID         uint64           `json:"worker_id"`
	PostStateRoot    []byte           `json:"post_state_root"`
	Proofs           []KeyValueProof  `json:"proofs"`
	ExecutionSummary struct {
		TxCount      uint32 `json:"tx_count"`
		SuccessCount uint32 `json:"success_count"`
		GasUsed      uint64 `json:"gas_used"`
	}`json:"execution_summary"`
	Signature       []byte `json:"signature"`
	TimestampUnixMs int64  `json:"timestamp_unix_ms"`
}

// BlockReceipt aggregates all batch receipts for a given block.
type BlockReceipt struct {
	BlockHeight     uint64         `json:"block_height"`
	BatchReceipts   []BatchReceipt `json:"batch_receipts"`
	FinalStateRoot  []byte         `json:"final_state_root"`
	Signature       []byte         `json:"signature"`
	TimestampUnixMs int64          `json:"timestamp_unix_ms"`
}

// ---------------------------
// Evidence
// ---------------------------

// Evidence represents fraud proof or misbehavior evidence.
type Evidence struct {
	ReporterID       uint64        `json:"reporter_id"`
	OffendingReceipt BatchReceipt  `json:"offending_receipt"`
	CounterProof     KeyValueProof `json:"counter_proof"`
	TimestampUnixMs  int64         `json:"timestamp_unix_ms"`
	Signature        []byte        `json:"signature"`
}

// ---------------------------
// Persistent Node State
// ---------------------------

type PersistentState struct {
	CurrentEpoch uint64 `json:"current_epoch"`
	NodeID       uint64 `json:"node_id"`
	PrivateKey   []byte `json:"private_key"`
	Stake        uint64 `json:"stake"`
	StateRoot    []byte `json:"state_root"`
}

// ---------------------------
// KVResult (intermediate execution result)
// ---------------------------

type KVResult struct {
	Index uint32
	Key   string
	Value []byte
}

// ---------------------------
// State Machine Interface
// ---------------------------

type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

// ---------------------------
// JSON Helpers
// ---------------------------

func (br *BatchReceipt) ToJSON() ([]byte, error) {
	return json.MarshalIndent(br, "", "  ")
}

func (br *BatchReceipt) FromJSON(data []byte) error {
	return json.Unmarshal(data, br)
}

func (brc *BlockReceipt) ToJSON() ([]byte, error) {
	return json.MarshalIndent(brc, "", "  ")
}

func (brc *BlockReceipt) FromJSON(data []byte) error {
	return json.Unmarshal(data, brc)
}
