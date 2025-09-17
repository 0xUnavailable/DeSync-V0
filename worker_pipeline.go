package desync

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
)

// stagingPool reduces allocations for worker staging maps.
var stagingPool = sync.Pool{
	New: func() any { return make(map[string][]byte, 128) },
}

// Single-threaded per-batch worker execution (staging).
func workerExecute(batch Batch) []KVResult {
	staging := stagingPool.Get().(map[string][]byte)
	for k := range staging {
		delete(staging, k)
	}

	sm := NewStagingStateMachine(staging)
	out, err := executeBatchStaged(batch, sm)
	if err != nil {
		fmt.Printf("worker %d execute error: %v\n", batch.WorkerID, err)
		out = make(map[string][]byte)
	}

	results := make([]KVResult, 0, len(batch.TxHashes))
	for i, idx := range batch.TxIndices {
		key := fmt.Sprintf("tx_%d", idx)
		if v, ok := out[key]; ok {
			c := make([]byte, len(v))
			copy(c, v)
			results = append(results, KVResult{
				Index: idx,
				Key:   key,
				Value: c,
			})
		} else {
			// skip
			_ = i
		}
	}

	stagingPool.Put(staging)
	return results
}

// Intra-node multithreaded execution: split batch across CPU cores deterministically.
func intraNodeExecute(batch Batch) []KVResult {
	numCores := runtime.NumCPU()
	chunkSize := (len(batch.TxHashes) + numCores - 1) / numCores
	resultsChan := make(chan []KVResult, numCores)
	var wg sync.WaitGroup

	for c := 0; c < numCores; c++ {
		start := c * chunkSize
		end := min((c+1)*chunkSize, len(batch.TxHashes))
		if start >= end {
			break
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			subBatch := Batch{
				BatchID:     batch.BatchID,
				BlockHeight: batch.BlockHeight,
				Sequence:    batch.Sequence,
				TxHashes:    batch.TxHashes[start:end],
				TxIndices:   batch.TxIndices[start:end],
				WorkerID:    batch.WorkerID,
			}
			staging := make(map[string][]byte)
			sm := NewStagingStateMachine(staging)
			out, _ := executeBatchStaged(subBatch, sm)

			localResults := []KVResult{}
			for _, idx := range subBatch.TxIndices {
				key := fmt.Sprintf("tx_%d", idx)
				if v, ok := out[key]; ok {
					// copy value so it is not mutated by other goroutines
					cpy := make([]byte, len(v))
					copy(cpy, v)
					localResults = append(localResults, KVResult{
						Index: idx, Key: key, Value: cpy,
					})
				}
			}
			resultsChan <- localResults
		}(start, end)
	}

	wg.Wait()
	close(resultsChan)

	// Merge all local results
	var allResults []KVResult
	for res := range resultsChan {
		allResults = append(allResults, res...)
	}

	// Sort deterministically by IndexInBlock
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Index < allResults[j].Index
	})
	return allResults
}

func mergeResultsOrdered(workerResults [][]KVResult) []KVResult {
	total := 0
	for _, wr := range workerResults {
		total += len(wr)
	}
	merged := make([]KVResult, 0, total)
	for _, wr := range workerResults {
		merged = append(merged, wr...)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Index < merged[j].Index
	})
	return merged
}

func applyResultsOrdered(db *StateDB, results []KVResult) error {
	// Ensure deterministic key order (lexicographic)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	b := db.NewBatch()
	for _, kv := range results {
		b.Put([]byte(kv.Key), kv.Value)
	}
	return db.WriteBatch(b)
}
