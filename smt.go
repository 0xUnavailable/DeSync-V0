package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

// ---------------------------
// Minimal Sparse Merkle Tree (SMT) prototype
// - Fixed depth (using SHA-256 bits, depth = 256)
// - Node storage persisted to StateDB (LevelDB)
// - Leaf hash = H(0x00 || keyHash || valueHash)
// - Node hash = H(0x01 || left || right)
// - Node DB keys: "smt:node:<pathBits>" where pathBits is ASCII "0101..."
// - Root DB key: "smt:root"
// Notes: This is a functional prototype for deterministically computing roots
// and proofs. For production, use a highly optimized library (IAVL/SMT lib).
// ---------------------------

const (
	// Depth (use 256 to map sha256 bits). You can reduce depth for tests.
	SMTDepth = 256

	// DB key prefixes
	smtNodePrefix = "smt:node:" // + pathbits
	smtRootKey    = "smt:root"
)

// SparseMerkleTree holds state for SMT; nodes persisted via StateDB
type SparseMerkleTree struct {
	db         *StateDB
	zeroHashes [][]byte // zero hash per level; zeroHashes[depth] = hash for empty subtree at that level
	depth      int
}

// NewSparseMerkleTree creates SMT backed by StateDB (db must be non-nil)
func NewSparseMerkleTree(db *StateDB, depth int) (*SparseMerkleTree, error) {
	if db == nil {
		return nil, fmt.Errorf("stateDB required")
	}
	if depth <= 0 || depth > SMTDepth {
		// cap depth
		depth = SMTDepth
	}
	smt := &SparseMerkleTree{
		db:    db,
		depth: depth,
	}
	smt.initZeroHashes()
	return smt, nil
}

// initZeroHashes precomputes empty subtree hashes bottom-up.
// zeroHashes[0] is the hash of empty leaf (H(0x00 || 0x00 || 0x00)).
// zeroHashes[level] corresponds to subtree of height level (leaf-level=0).
func (s *SparseMerkleTree) initZeroHashes() {
	d := s.depth
	s.zeroHashes = make([][]byte, d+1)
	// zero leaf base (we'll use empty keyHash/valueHash)
	h := sha256.New()
	h.Write([]byte{0x00})
	h.Write([]byte{}) // empty keyHash
	h.Write([]byte{}) // empty valueHash
	s.zeroHashes[0] = h.Sum(nil)
	// build up
	for i := 1; i <= d; i++ {
		h2 := sha256.New()
		h2.Write([]byte{0x01})
		h2.Write(s.zeroHashes[i-1])
		h2.Write(s.zeroHashes[i-1])
		s.zeroHashes[i] = h2.Sum(nil)
	}
}

// helper: db key for node at path (path as string of '0'/'1')
func nodeDBKey(path string) []byte {
	return []byte(smtNodePrefix + path)
}

// helper: load node hash for path; if not found, return zeroHashes[level]
func (s *SparseMerkleTree) loadNodeHash(path string, level int) ([]byte, error) {
	// level: height from leaf; leaf level = 0; root level = depth
	// path length == level bits from root? We'll use path as bits from root to node.
	// If not present, return zeroHashes[remainingHeight]
	b, err := s.db.Get(string(nodeDBKey(path)))
	if err != nil {
		// treat not found as zero
		if err == leveldb.ErrNotFound {
			// If path corresponds to a subtree at height remaining = depth - len(path)
			remaining := s.depth - len(path)
			if remaining >= 0 && remaining <= s.depth {
				return s.zeroHashes[remaining], nil
			}
			// fallback
			return s.zeroHashes[s.depth], nil
		}
		return nil, err
	}
	return append([]byte(nil), b...), nil
}

// persistNodeHash stores node hash at path in batch
func persistNodeHashToBatch(batch *leveldb.Batch, path string, hash []byte) {
	key := nodeDBKey(path)
	batch.Put(key, hash)
}

// SetRoot persists the root (under a known key) and also persists updated nodes (done in Update)
func (s *SparseMerkleTree) saveRootInDB(root []byte, batch *leveldb.Batch) {
	batch.Put([]byte(smtRootKey), root)
}

// GetRoot loads the persisted root if present, else returns zero root
func (s *SparseMerkleTree) GetRoot() ([]byte, error) {
	v, err := s.db.Get(smtRootKey)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return s.zeroHashes[s.depth], nil
		}
		return nil, err
	}
	if v == nil {
		return s.zeroHashes[s.depth], nil
	}
	return append([]byte(nil), v...), nil
}

// Update updates key->value in SMT and persists changed nodes using a LevelDB batch.
// Returns new root.
func (s *SparseMerkleTree) Update(key, value []byte) ([]byte, error) {
	// derive keyHash and valueHash
	keyHash := sha256.Sum256(key)
	valueHash := sha256.Sum256(value)

	// leaf hash
	leafH := sha256.New()
	leafH.Write([]byte{0x00})
	leafH.Write(keyHash[:])
	leafH.Write(valueHash[:])
	leafHash := leafH.Sum(nil)

	// We'll walk from leaf up to root, computing hashes and store them into a LevelDB batch.
	batch := s.db.NewBatch()

	// path bits derived from keyHash; we use MSB -> LSB across depth bits.
	pathBits := bytesToBits(keyHash[:], s.depth) // string of '0'/'1' length = depth

	// At leaf, path = full pathBits
	// We'll store node hashes keyed by path prefix from root down; but to compute parents
	// we need leaf sibling at each level. We'll compute bottom-up.
	// To keep keys short, we will store node at pathPrefix where pathPrefix is bits from root to node.
	// For leaf node, the path is full 'pathBits'.
	// level 0 = leaf level; level d = root.

	// currentHash is hash for subtree at current path (starting at leaf -> level 0)
	currentHash := leafHash
	// store leaf as node at full path
	leafPath := pathBits // full depth bits
	persistNodeHashToBatch(batch, leafPath, currentHash)

	// ascend
	// for level = 1..depth (level is #bits from leaf), at each step compute parent hash.
	for level := 1; level <= s.depth; level++ {
		// sibling bit is the bit at position depth - level (counting from 0)
		// parent path = prefix of pathBits of length s.depth - level
		bitIndex := s.depth - level
		bit := pathBits[bitIndex] // '0' or '1'

		// sibling path: prefix + opposite bit + (remaining bits don't matter for node key)
		parentPrefix := pathBits[:bitIndex] // path from root to parent
		var leftHash, rightHash []byte

		// sibling path (full leaf sibling) would be parentPrefix + opposite + suffix. But we only store node hash for that subtree's root.
		// sibling node stored path key we expect to find is parentPrefix + opposite + (all children bits zero?) -- but we stored nodes at full leaf path and parent internal nodes at shorter prefixes. To load sibling hash we try to load node at parentPrefix + opposite prefix-of-child.
		// For simplicity we look for the immediate child node at path parentPrefix + "0" or "1".
		var child0Path, child1Path string
		child0Path = parentPrefix + "0"
		child1Path = parentPrefix + "1"

		// load child hashes (if not exist, use zero for remaining height)
		h0, err := s.loadNodeHash(child0Path, s.depth-level)
		if err != nil {
			return nil, err
		}
		h1, err := s.loadNodeHash(child1Path, s.depth-level)
		if err != nil {
			return nil, err
		}

		if bit == '0' {
			// current leaf is in left subtree; currentHash should match h0; but we already have currentHash computed.
			leftHash = currentHash
			rightHash = h1
		} else {
			leftHash = h0
			rightHash = currentHash
		}

		// compute parent
		hp := sha256.New()
		hp.Write([]byte{0x01})
		hp.Write(leftHash)
		hp.Write(rightHash)
		parentHash := hp.Sum(nil)

		// persist parent at prefix (node representing subtree rooted at parentPrefix)
		persistNodeHashToBatch(batch, parentPrefix, parentHash)

		// move currentHash up
		currentHash = parentHash
	}

	// currentHash now root
	root := currentHash
	// persist root
	s.saveRootInDB(root, batch)

	// write batch atomically
	if err := s.db.WriteBatch(batch); err != nil {
		return nil, err
	}
	return append([]byte(nil), root...), nil
}

// GenerateProof returns sibling hashes from leaf up to root for the given key.
// The proof order is bottom-up: proof[0] = sibling at leaf level, proof[depth-1] = sibling near root.
func (s *SparseMerkleTree) GenerateProof(key []byte) ([][]byte, error) {
	keyHash := sha256.Sum256(key)
	pathBits := bytesToBits(keyHash[:], s.depth)

	proof := make([][]byte, 0, s.depth)
	// At each level, sibling is child at parentPrefix + opposite bit
	for level := 0; level < s.depth; level++ {
		bitIndex := s.depth - 1 - level // at leaf level level=0 -> bitIndex = depth-1
		parentPrefix := ""
		if bitIndex > 0 {
			parentPrefix = pathBits[:bitIndex]
		}
		// sibling bit is opposite of pathBits[bitIndex]
		var siblingPath string
		if pathBits[bitIndex] == '0' {
			siblingPath = parentPrefix + "1"
		} else {
			siblingPath = parentPrefix + "0"
		}
		// sibling subtree height = level (distance from leaf)
		sibHash, err := s.loadNodeHash(siblingPath, level)
		if err != nil {
			return nil, err
		}
		proof = append(proof, sibHash)
	}
	return proof, nil
}

// Helper: convert bytes to string of '0'/'1' bits length = depth. Use MSB-first.
func bytesToBits(b []byte, depth int) string {
	var sb strings.Builder
	sb.Grow(depth)
	bitCount := 0
	for i := 0; i < len(b) && bitCount < depth; i++ {
		for j := 7; j >= 0 && bitCount < depth; j-- {
			bit := (b[i] >> uint(j)) & 1
			if bit == 0 {
				sb.WriteByte('0')
			} else {
				sb.WriteByte('1')
			}
			bitCount++
		}
	}
	// if depth > 8*len(b), pad zeros
	for bitCount < depth {
		sb.WriteByte('0')
		bitCount++
	}
	return sb.String()
}

// Helper: verify proof for a given key/value and expected root
// proof: sibling hashes bottom-up
func VerifyProof(key, value []byte, proof [][]byte, depth int, expectedRoot []byte) bool {
	keyHash := sha256.Sum256(key)
	valueHash := sha256.Sum256(value)
	leafH := sha256.New()
	leafH.Write([]byte{0x00})
	leafH.Write(keyHash[:])
	leafH.Write(valueHash[:])
	hash := leafH.Sum(nil) // leaf-level hash

	// pathbits
	pathBits := bytesToBits(keyHash[:], depth)

	// For level 0..depth-1:
	for level := 0; level < depth; level++ {
		bitIndex := depth - 1 - level
		sibling := proof[level]
		if pathBits[bitIndex] == '0' {
			// current is left
			hp := sha256.New()
			hp.Write([]byte{0x01})
			hp.Write(hash)
			hp.Write(sibling)
			hash = hp.Sum(nil)
		} else {
			hp := sha256.New()
			hp.Write([]byte{0x01})
			hp.Write(sibling)
			hp.Write(hash)
			hash = hp.Sum(nil)
		}
	}
	// final hash should equal expectedRoot
	return bytes.Equal(hash, expectedRoot)
}

// DumpNodeHex returns printed node key->value hex map for debugging (reads all nodes via IterateAll, filters smt prefix)
func (s *SparseMerkleTree) DumpNodeHex() (map[string]string, error) {
	m, err := s.db.IterateAll("")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for k, v := range m {
		if strings.HasPrefix(k, smtNodePrefix) || k == smtRootKey {
			out[k] = hex.EncodeToString(v)
		}
	}
	return out, nil
}
