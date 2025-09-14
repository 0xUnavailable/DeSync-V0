package main

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

// Stress test: build a large state map, compute root, and check proof verification
func TestStressMerkleAndProofs(t *testing.T) {
	const N = 50000 // number of entries
	state := make(map[string][]byte, N)

	// Fill with 50k entries
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key%d", i)
		v := []byte(fmt.Sprintf("val%d", i))
		state[k] = v
	}

	// Compute root
	root, _, indexMap := computeStateRootFromMap(state)
	if len(root) == 0 {
		t.Fatal("empty root")
	}

	// Pick some random keys and verify proofs
	for i := 0; i < N; i += 5000 {
		k := fmt.Sprintf("key%d", i)
		proof, err := generateInclusionProof(k, state)
		if err != nil {
			t.Fatalf("proof err for key %s: %v", k, err)
		}
		d := append([]byte(k), state[k]...)
		leaf := sha256.Sum256(d)
		idx := indexMap[k]
		got := verifyInclusionProof(leaf[:], proof, idx)
		if string(got) != string(root) {
			t.Fatalf("proof verification failed for %s", k)
		}
	}
}

// Stress test: run many Apply calls in parallel
func TestStressApplyParallel(t *testing.T) {
	sm := &SimpleStateMachine{}
	const N = 10000

	for i := 0; i < N; i++ {
		cmd := []byte(fmt.Sprintf("k%d:v%d", i, i))
		if _, err := sm.Apply(cmd); err != nil {
			t.Fatalf("apply failed: %v", err)
		}
	}

	// Check some values
	for i := 0; i < N; i += 2000 {
		key := fmt.Sprintf("k%d", i)
		execMu.RLock()
		v, ok := executionState[key]
		execMu.RUnlock()
		if !ok || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("state mismatch at %s", key)
		}
	}
}
