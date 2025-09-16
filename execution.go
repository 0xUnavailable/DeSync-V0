package main

import (
	"crypto/sha256"
	"errors"
	"sort"
)

// sortedKeysFromMap returns lexicographically sorted keys for deterministic iteration.
func sortedKeysFromMap(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// computeStateRootFromMap builds a simple Merkle root from (sorted) key||value leaves.
// Returns (root, concatenatedLeaves, indexOfKey)
func computeStateRootFromMap(state map[string][]byte) ([]byte, []byte, map[string]int) {
	keys := sortedKeysFromMap(state)
	leaves := make([][]byte, 0, len(keys))
	indexOf := make(map[string]int)
	for i, k := range keys {
		d := append([]byte(k), state[k]...)
		h := sha256.Sum256(d)
		leaves = append(leaves, h[:])
		indexOf[k] = i
	}
	if len(leaves) == 0 {
		empty := sha256.Sum256([]byte{})
		return empty[:], nil, indexOf
	}
	nodes := leaves
	for len(nodes) > 1 {
		var next [][]byte
		for i := 0; i < len(nodes); i += 2 {
			if i+1 == len(nodes) {
				h := sha256.Sum256(append(nodes[i], nodes[i]...))
				next = append(next, h[:])
			} else {
				concat := append(nodes[i], nodes[i+1]...)
				h := sha256.Sum256(concat)
				next = append(next, h[:])
			}
		}
		nodes = next
	}
	root := nodes[0]
	leavesConcat := bytesConcat(leaves)
	return root, leavesConcat, indexOf
}

func bytesConcat(arr [][]byte) []byte {
	var out []byte
	for _, a := range arr {
		out = append(out, a...)
	}
	return out
}

// computeStateRootFromDB materializes DB (prototype) and computes root.
func computeStateRootFromDB(db *StateDB) ([]byte, error) {
	m, err := db.IterateAll("")
	if err != nil {
		return nil, err
	}
	delete(m, "persistent_state")
	root, _, _ := computeStateRootFromMap(m)
	return root, nil
}

func generateInclusionProofFromDB(db *StateDB, key string) ([][]byte, error) {
	m, err := db.IterateAll("")
	if err != nil {
		return nil, err
	}
	delete(m, "persistent_state")
	return generateInclusionProof(key, m)
}

// generateInclusionProof constructs a simple Merkle sibling list for the given key.
func generateInclusionProof(key string, state map[string][]byte) ([][]byte, error) {
	keys := sortedKeysFromMap(state)
	index := -1
	for i, k := range keys {
		if k == key {
			index = i
			break
		}
	}
	if index == -1 {
		return nil, errors.New("key-not-found")
	}
	leaves := make([][]byte, 0, len(keys))
	for _, k := range keys {
		d := append([]byte(k), state[k]...)
		h := sha256.Sum256(d)
		leaves = append(leaves, h[:])
	}
	proof := make([][]byte, 0)
	i := index
	nodes := leaves
	for len(nodes) > 1 {
		var next [][]byte
		for j := 0; j < len(nodes); j += 2 {
			var left, right []byte
			left = nodes[j]
			if j+1 < len(nodes) {
				right = nodes[j+1]
			} else {
				right = nodes[j]
			}
			if j == i || j+1 == i {
				if j == i {
					proof = append(proof, right)
				} else {
					proof = append(proof, left)
				}
			}
			concat := append(left, right...)
			h := sha256.Sum256(concat)
			next = append(next, h[:])
		}
		i = i / 2
		nodes = next
	}
	return proof, nil
}

// verifyInclusionProof re-computes the root from leaf + proof (prototype).
func verifyInclusionProof(leafHash []byte, proof [][]byte, index int) []byte {
	hash := leafHash
	i := index
	for _, sibling := range proof {
		if i%2 == 0 {
			concat := append(hash, sibling...)
			h := sha256.Sum256(concat)
			hash = h[:]
		} else {
			concat := append(sibling, hash...)
			h := sha256.Sum256(concat)
			hash = h[:]
		}
		i = i / 2
	}
	return hash
}
