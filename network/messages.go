package network

import (
	"encoding/json"

	"github.com/MelzLabs/DeSync/types"
)

// === Worker hello ===
type WorkerHello struct {
	WorkerID string
	PubKey   []byte
	PeerID   string
}

func EncodeWorkerHello(wh WorkerHello) ([]byte, error) {
	return json.Marshal(wh)
}

func DecodeWorkerHello(data []byte) (*WorkerHello, error) {
	var wh WorkerHello
	if err := json.Unmarshal(data, &wh); err != nil {
		return nil, err
	}
	return &wh, nil
}

// === Leader announce ===
type LeaderAnnounce struct {
	LeaderID string
	Epoch    int
}

func EncodeLeaderAnnounce(la LeaderAnnounce) ([]byte, error) {
	return json.Marshal(la)
}

func DecodeLeaderAnnounce(data []byte) (*LeaderAnnounce, error) {
	var la LeaderAnnounce
	if err := json.Unmarshal(data, &la); err != nil {
		return nil, err
	}
	return &la, nil
}

// === Tx message ===
type TxMessage struct {
	Tx *types.Transaction
}

func EncodeTx(tx TxMessage) ([]byte, error) {
	return json.Marshal(tx)
}

func DecodeTx(data []byte) (*TxMessage, error) {
	var tx TxMessage
	if err := json.Unmarshal(data, &tx); err != nil {
		return nil, err
	}
	return &tx, nil
}

// === Batch message ===
// ✅ Added Snapshot so leader can send state to workers
type BatchMessage struct {
	BatchID      string
	Txs          []*types.Transaction
	ShardIndex   int
	ShardCount   int
	TargetWorker string

	Snapshot map[string][]byte
}

func Encode(bm BatchMessage) ([]byte, error) {
	return json.Marshal(bm)
}

func DecodeBatch(data []byte) (*BatchMessage, error) {
	var bm BatchMessage
	if err := json.Unmarshal(data, &bm); err != nil {
		return nil, err
	}
	return &bm, nil
}

// === Signed results ===
type SignedResult struct {
	BatchID    string
	WorkerID   string
	ShardIndex int
	ShardCount int
	Results    []*types.TxResult
	PubKey     []byte
	Sig        []byte
}

func EncodeResult(sr SignedResult) ([]byte, error) {
	return json.Marshal(sr)
}

func DecodeResult(data []byte) (*SignedResult, error) {
	var sr SignedResult
	if err := json.Unmarshal(data, &sr); err != nil {
		return nil, err
	}
	return &sr, nil
}
