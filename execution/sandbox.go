package execution

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/MelzLabs/DeSync/types"
)

// SandboxedState simple key->value map for sandbox execution (thread-safe)
type SandboxedState struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewSandboxedState(snapshot map[string][]byte) *SandboxedState {
	s := &SandboxedState{data: make(map[string][]byte)}
	if snapshot != nil {
		for k, v := range snapshot {
			cp := make([]byte, len(v))
			copy(cp, v)
			s.data[k] = cp
		}
	}
	return s
}

func (s *SandboxedState) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true
}

func (s *SandboxedState) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.data[key] = cp
}

// GetSnapshot returns a copy of all data (thread-safe)
func (s *SandboxedState) GetSnapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		snapshot[k] = cp
	}
	return snapshot
}

// DeclaredKeysFor returns deterministic keys touched by tx.
func DeclaredKeysFor(tx *types.Transaction) []string {
	switch tx.Action {
	case "AllocateLand":
		plotID := tx.Payload["plotID"]
		return []string{
			fmt.Sprintf("land:%s:owner", plotID),
			fmt.Sprintf("land:%s:quota", plotID),
			fmt.Sprintf("land:%s:resource", plotID),
		}
	case "Grow":
		plotID := tx.Payload["plotID"]
		return []string{
			fmt.Sprintf("land:%s:resource", plotID),
		}
	case "Transfer":
		from := tx.Payload["from"]
		to := tx.Payload["to"]
		return []string{
			fmt.Sprintf("player:%s:balance", from),
			fmt.Sprintf("player:%s:balance", to),
		}
	case "PayTax":
		plotID := tx.Payload["plotID"]
		return []string{
			fmt.Sprintf("land:%s:owner", plotID),
			fmt.Sprintf("player:%s:balance", tx.Sender),
			fmt.Sprintf("land:%s:tax_paid", plotID),
		}
	default:
		return []string{fmt.Sprintf("player:%s:balance", tx.Sender)}
	}
}

// ApplyTransaction executes tx in sandbox and returns a TxResult.
func ApplyTransaction(snap *SandboxedState, tx *types.Transaction, workerID string, index int) *types.TxResult {
	res := &types.TxResult{
		TxID:         tx.ID,
		Index:        index,
		Success:      false,
		WriteSet:     make(map[string][]byte),
		ExecutedBy:   workerID,
		DeclaredKeys: DeclaredKeysFor(tx),
	}


	switch tx.Action {
	case "AllocateLand":
		plotID := tx.Payload["plotID"]
		owner := tx.Payload["owner"]
		if plotID == "" || owner == "" {
			res.Error = "missing plotID or owner"
			return res
		}
		key := fmt.Sprintf("land:%s:owner", plotID)
		curB, _ := snap.Get(key)
		if curB != nil && string(curB) != "" {
			// plot already has an owner
			res.Error = "plot already allocated"
			return res
		}
		// mark ownership and reset counters
		res.WriteSet[key] = []byte(owner)
		res.WriteSet[fmt.Sprintf("land:%s:quota", plotID)] = []byte("0")
		res.WriteSet[fmt.Sprintf("land:%s:resource", plotID)] = []byte("0")
		res.Events = append(res.Events, "Allocated:"+plotID)
		res.Success = true
		return res

	case "Grow":
		plotID := tx.Payload["plotID"]
		amtStr := tx.Payload["amount"]
		if plotID == "" || amtStr == "" {
			res.Error = "missing plotID or amount"
			return res
		}
		amt, err := strconv.Atoi(amtStr)
		if err != nil || amt <= 0 {
			res.Error = "invalid amount"
			return res
		}
		key := fmt.Sprintf("land:%s:resource", plotID)
		curB, _ := snap.Get(key)
		cur := 0
		if curB != nil {
			cur, _ = strconv.Atoi(string(curB))
		}
		newVal := cur + amt
		res.WriteSet[key] = []byte(strconv.Itoa(newVal))
		res.Events = append(res.Events, fmt.Sprintf("Grew:%s:%d", plotID, amt))
		res.Success = true
		return res

	case "Transfer":
    from := tx.Payload["from"]
    to := tx.Payload["to"]
    amtStr := tx.Payload["amount"]
    if from == "" || to == "" || amtStr == "" {
        res.Error = "missing from/to/amount"
        return res
    }
    amt, err := strconv.Atoi(amtStr)
    if err != nil || amt <= 0 {
        res.Error = "invalid amount"
        return res
    }

    fromKey := fmt.Sprintf("player:%s:balance", from)
    toKey := fmt.Sprintf("player:%s:balance", to)

    // Normal transfer flow for all accounts
    fromB, _ := snap.Get(fromKey)
    fromBal := 0
    if fromB != nil {
        fromBal, _ = strconv.Atoi(string(fromB))
    }
    if fromBal < amt {
        res.Error = "insufficient balance"
        return res
    }
    toB, _ := snap.Get(toKey)
    toBal := 0
    if toB != nil {
        toBal, _ = strconv.Atoi(string(toB))
    }
    res.WriteSet[fromKey] = []byte(strconv.Itoa(fromBal - amt))
    res.WriteSet[toKey] = []byte(strconv.Itoa(toBal + amt))
    res.Events = append(res.Events, fmt.Sprintf("Transfer:%s->%s:%d", from, to, amt))
    res.Success = true
    return res

	case "PayTax":
		plotID := tx.Payload["plotID"]
		amountStr := tx.Payload["amount"]
		if plotID == "" || amountStr == "" {
			res.Error = "missing plotID/amount"
			return res
		}
		amt, err := strconv.Atoi(amountStr)
		if err != nil || amt < 0 {
			res.Error = "invalid amount"
			return res
		}
		ownerKey := fmt.Sprintf("land:%s:owner", plotID)
		ownerB, ok := snap.Get(ownerKey)
		if !ok || ownerB == nil || string(ownerB) == "" {
			res.Error = "plot not allocated"
			return res
		}
		owner := string(ownerB)
		ownerBalKey := fmt.Sprintf("player:%s:balance", owner)
		bB, _ := snap.Get(ownerBalKey)
		bal := 0
		if bB != nil {
			bal, _ = strconv.Atoi(string(bB))
		}
		if bal < amt {
			res.Error = "insufficient balance to pay tax"
			return res
		}
		res.WriteSet[ownerBalKey] = []byte(strconv.Itoa(bal - amt))
		res.WriteSet[fmt.Sprintf("land:%s:tax_paid", plotID)] = []byte("true")
		res.Events = append(res.Events, fmt.Sprintf("TaxPaid:%s:%d", plotID, amt))
		res.Success = true
		return res

	default:
		res.Error = "unknown action"
		return res
	}
}
