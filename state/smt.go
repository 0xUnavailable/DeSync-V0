package state

import (
	"crypto/sha256"
	"sync"

	celsmt "github.com/celestiaorg/smt"
)

// SparseMerkleTree wraps celestiaorg/smt with an in-memory KV mirror.
type SparseMerkleTree struct {
	tree *celsmt.SparseMerkleTree

	mu sync.RWMutex
	kv map[string][]byte
}

// NewSparseMerkleTree creates a new in-memory SMT.
func NewSparseMerkleTree() *SparseMerkleTree {
	store := celsmt.NewSimpleMap()
	tree := celsmt.NewSparseMerkleTree(store, store, sha256.New())
	return &SparseMerkleTree{
		tree: tree,
		kv:   make(map[string][]byte),
	}
}

// Update inserts/updates a key and mirrors it in kv.
func (s *SparseMerkleTree) Update(key, value []byte) error {
	if _,err := s.tree.Update(key, value); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.kv[string(key)] = cp
	return nil
}

// GetRoot returns the Merkle root.
func (s *SparseMerkleTree) GetRoot() []byte {
	return s.tree.Root()
}

// Prove generates a Merkle proof for a key.
func (s *SparseMerkleTree) Prove(key []byte) (celsmt.SparseMerkleProof, error) {
	return s.tree.Prove(key)
}

// VerifyProof verifies a proof against a root.
func VerifyProof(proof celsmt.SparseMerkleProof, root, key, value []byte) bool {
	return celsmt.VerifyProof(proof, root, key, value, sha256.New())
}

// ExportSnapshot returns a copy of the in-memory kv.
func (s *SparseMerkleTree) ExportSnapshot() map[string][]byte {
	out := make(map[string][]byte)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.kv {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}
