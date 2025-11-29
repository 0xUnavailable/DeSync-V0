package execution

import (
	"strconv"
	"github.com/MelzLabs/DeSync/types"
)

// // ApplyTransaction applies a transaction to a sandboxed state
// func ApplyTransaction(snap *SandboxedState, tx *types.Transaction, workerID string, idx int) types.TxResult {
// 	if err := tx.VerifySignature(); err != nil {
// 		return types.TxResult{TxID: tx.ID, Success: false, Error: err.Error()}
// 	}

// 	switch tx.Action {
// 	case "AllocateLand":
// 		return applyAllocateLand(snap, tx)
// 	case "Grow":
// 		return applyGrow(snap, tx)
// 	default:
// 		return types.TxResult{TxID: tx.ID, Success: false, Error: "unknown action"}
// 	}
// }

func applyAllocateLand(snap *SandboxedState, tx *types.Transaction) types.TxResult {
	plotID := tx.Payload["plotID"]
	owner := tx.Payload["owner"]

	key := "land:" + plotID
	if _, exists := snap.Get(key); exists {
		return types.TxResult{TxID: tx.ID, Success: false, Error: "plot already owned"}
	}
	snap.Set(key, []byte(owner))
	return types.TxResult{TxID: tx.ID, Success: true}
}

func applyGrow(snap *SandboxedState, tx *types.Transaction) types.TxResult {
	plotID := tx.Payload["plotID"]
	amountStr := tx.Payload["amount"]
	amount, err := strconv.Atoi(amountStr)
	if err != nil {
		return types.TxResult{TxID: tx.ID, Success: false, Error: "invalid amount"}
	}
	key := "growth:" + plotID
	prevVal, _ := snap.Get(key)
	prevAmt := 0
	if prevVal != nil {
		prevAmt, _ = strconv.Atoi(string(prevVal))
	}
	newAmt := prevAmt + amount
	snap.Set(key, []byte(strconv.Itoa(newAmt)))
	return types.TxResult{TxID: tx.ID, Success: true}
}

// func NewSandboxedState(mem *state.MemStore) *SandboxedState {
// 	if mem == nil {
// 		mem = state.NewMemStore()
// 	}
// 	return &SandboxedState{mem: mem}
// }

// func (s *SandboxedState) Get(key string) ([]byte, bool) {
// 	return s.mem.Get(key)
// }

// func (s *SandboxedState) Set(key string, val []byte) {
// 	if err := s.mem.Set(key, val); err != nil {
// 		log.Printf("failed to set key=%s: %v", key, err)
// 	}
// }
