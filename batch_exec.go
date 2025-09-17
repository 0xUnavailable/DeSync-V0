package desync

import (
	"crypto/sha256"
	"fmt"
)

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
	// iterate staging deterministically when copying
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
		m[k] = v
		res.Changes[k] = v
	}

	root, _, _ := computeStateRootFromMap(m)
	res.PostStateRoot = root
	res.ExecutionSummary.TxCount = txCount
	res.ExecutionSummary.SuccessCount = successCount
	res.ExecutionSummary.GasUsed = gasUsed

	return res, nil
}
