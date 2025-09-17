package desync

import (
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

// GenerateBatchReceiptAfterCommit generates a BatchReceipt for the provided
// BatchResult. It MUST be called after the batch's changes have been written
// and the SMT root persisted in the StateDB (i.e. commit already applied).
func GenerateBatchReceiptAfterCommit(db *StateDB, res BatchResult) (*BatchReceipt, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}

	// Confirm that the DB's current SMT root matches the batch result root.
	dbRoot, err := db.CurrentRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to read current SMT root: %w", err)
	}
	if !byteSlicesEqual(dbRoot, res.PostStateRoot) {
		return nil, fmt.Errorf("root mismatch: db root %s != result root %s",
			hex.EncodeToString(dbRoot), hex.EncodeToString(res.PostStateRoot))
	}

	// Determine keys to produce receipts for: use deterministic sorted keys from res.Changes.
	changeKeys := sortedKeysFromMap(res.Changes)

	receipt := &BatchReceipt{
		BatchID:       res.BatchID,
		WorkerID:      res.WorkerID,
		PostStateRoot: append([]byte(nil), res.PostStateRoot...),
		TimestampUnixMs: time.Now().UnixNano() / int64(time.Millisecond),
		Proofs:        make([]KeyValueProof, 0, len(changeKeys)),
		ExecutionSummary: struct {
			TxCount      uint32 `json:"tx_count"`
			SuccessCount uint32 `json:"success_count"`
			GasUsed      uint64 `json:"gas_used"`
		}{
			TxCount:      res.ExecutionSummary.TxCount,
			SuccessCount: res.ExecutionSummary.SuccessCount,
			GasUsed:      res.ExecutionSummary.GasUsed,
		},
		Signature: res.Signature,
	}

	for _, k := range changeKeys {
		// fetch SMT proof
		proofHashes, err := db.SMTGenerateProof([]byte(k))
		if err != nil {
			return nil, fmt.Errorf("failed to generate SMT proof for key %s: %w", k, err)
		}

		newVal := res.Changes[k]

		receipt.Proofs = append(receipt.Proofs, KeyValueProof{
			Key:         []byte(k),
			Value:       append([]byte(nil), newVal...),
			MerkleProof: proofHashes,
			Root:        append([]byte(nil), res.PostStateRoot...),
		})
	}

	// Ensure deterministic order
	sort.Slice(receipt.Proofs, func(i, j int) bool {
		return string(receipt.Proofs[i].Key) < string(receipt.Proofs[j].Key)
	})

	return receipt, nil
}

// VerifyBatchReceipt verifies all inclusion proofs contained in a BatchReceipt.
func VerifyBatchReceipt(db *StateDB, receipt *BatchReceipt) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if receipt == nil {
		return fmt.Errorf("receipt is nil")
	}

	depth := db.depth

	// Sanity: ensure DB root matches receipt root.
	dbRoot, err := db.CurrentRoot()
	if err != nil {
		return fmt.Errorf("failed to read SMT root from DB: %w", err)
	}
	if !byteSlicesEqual(dbRoot, receipt.PostStateRoot) {
		return fmt.Errorf("root mismatch between DB (%s) and receipt (%s)",
			hex.EncodeToString(dbRoot), hex.EncodeToString(receipt.PostStateRoot))
	}

	for _, p := range receipt.Proofs {
		if p.Value == nil {
			return fmt.Errorf("missing Value for key %s in receipt", string(p.Key))
		}
		verified := VerifyProof(p.Key, p.Value, p.MerkleProof, depth, receipt.PostStateRoot)
		if !verified {
			return fmt.Errorf("proof verification failed for key %s", string(p.Key))
		}
	}

	return nil
}

// helper
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
