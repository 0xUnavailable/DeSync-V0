package main

import (
	"crypto/sha256"
	"encoding/hex"
)

// ---------------------------
// Utilities
// ---------------------------

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func computeTxID(txData []byte) []byte {
	h := sha256.Sum256(txData)
	return h[:]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
