package state

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSMTRoundTrip_MultipleKeys(t *testing.T) {
	s := NewSparseMerkleTree()

	// Insert 10 keys and values
	keys := make([][]byte, 10)
	values := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("val-%d", i))
		s.Update(keys[i], values[i])
	}

	root := s.GetRoot()
	if len(root) == 0 {
		t.Fatal("expected non-empty root after inserts")
	}

	// Verify all proofs
	for i := 0; i < 10; i++ {
		proof, err := s.Prove(keys[i])
		if err != nil {
			t.Fatalf("failed to prove key-%d: %v", i, err)
		}
		if !VerifyProof(proof, root, keys[i], values[i]) {
			t.Fatalf("proof failed for key-%d", i)
		}
	}

	// Negative test: wrong value must fail
	proof, _ := s.Prove(keys[0])
	if VerifyProof(proof, root, keys[0], []byte("wrong-val")) {
		t.Fatal("expected failure with wrong value")
	}
}

func TestSMT_EmptyRootDeterminism(t *testing.T) {
	s1 := NewSparseMerkleTree()
	s2 := NewSparseMerkleTree()

	root1 := s1.GetRoot()
	root2 := s2.GetRoot()

	if !bytes.Equal(root1, root2) {
		t.Fatal("empty roots should match")
	}
}

func TestSMT_ConsistencyAfterUpdate(t *testing.T) {
	s := NewSparseMerkleTree()

	key := []byte("land-123")
	val1 := []byte("owner-A")
	val2 := []byte("owner-B")

	s.Update(key, val1)
	root1 := s.GetRoot()

	proof1, _ := s.Prove(key)
	if !VerifyProof(proof1, root1, key, val1) {
		t.Fatal("proof for val1 failed")
	}

	// Update same key with new value
	s.Update(key, val2)
	root2 := s.GetRoot()

	if bytes.Equal(root1, root2) {
		t.Fatal("root should change after update")
	}

	proof2, _ := s.Prove(key)
	if !VerifyProof(proof2, root2, key, val2) {
		t.Fatal("proof for val2 failed after update")
	}
}

func TestSMT_IndependentKeys(t *testing.T) {
	s := NewSparseMerkleTree()

	keyA, valA := []byte("plot-A"), []byte("val-A")
	keyB, valB := []byte("plot-B"), []byte("val-B")

	s.Update(keyA, valA)
	root1 := s.GetRoot()

	s.Update(keyB, valB)
	root2 := s.GetRoot()

	if bytes.Equal(root1, root2) {
		t.Fatal("roots should differ after adding new key")
	}

	// Both proofs must verify against the latest root
	proofA, _ := s.Prove(keyA)
	if !VerifyProof(proofA, root2, keyA, valA) {
		t.Fatal("proof for A failed on updated root")
	}

	proofB, _ := s.Prove(keyB)
	if !VerifyProof(proofB, root2, keyB, valB) {
		t.Fatal("proof for B failed on updated root")
	}
}
