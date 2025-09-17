package desync

import (
	"encoding/hex"
	"sort"
)

// ---------------------------
// Deterministic ordering helper
// ---------------------------

func txLess(a, b Transaction) bool {
	if a.BlockHeight != b.BlockHeight {
		return a.BlockHeight < b.BlockHeight
	}
	if a.IndexInBlock != b.IndexInBlock {
		return a.IndexInBlock < b.IndexInBlock
	}
	return hex.EncodeToString(a.TxID) < hex.EncodeToString(b.TxID)
}

func orderTransactionsDeterministic(txs []Transaction) []Transaction {
	sort.SliceStable(txs, func(i, j int) bool {
		return txLess(txs[i], txs[j])
	})
	return txs
}

// ---------------------------
// Batch creation
// ---------------------------

func createSequentialBatches(ordered []Transaction, activeWorkers []uint64, maxBatchSize int, startSequence uint64, prevStateRoot []byte) []Batch {
	batches := []Batch{}
	if len(activeWorkers) == 0 {
		return batches
	}
	var batchTxHashes [][]byte
	var batchTxIndices []uint32
	seq := startSequence
	workerCount := len(activeWorkers)
	for i, tx := range ordered {
		batchTxHashes = append(batchTxHashes, tx.TxID)
		batchTxIndices = append(batchTxIndices, tx.IndexInBlock)
		if len(batchTxHashes) >= maxBatchSize || i == len(ordered)-1 {
			worker := activeWorkers[(i/maxBatchSize)%workerCount]
			b := Batch{
				BatchID:       seq,
				BlockHeight:   ordered[0].BlockHeight,
				Sequence:      seq,
				TxHashes:      batchTxHashes,
				TxIndices:     batchTxIndices,
				PrevStateRoot: prevStateRoot,
				WorkerID:      worker,
			}
			batches = append(batches, b)
			seq++
			batchTxHashes = nil
			batchTxIndices = nil
		}
	}
	return batches
}
