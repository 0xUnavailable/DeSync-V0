package main


// ---------------------------
// Types
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

type KVResult struct {
	Index uint32
	Key   string
	Value []byte
}

// StateMachine is a generic interface for execution engines
type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

