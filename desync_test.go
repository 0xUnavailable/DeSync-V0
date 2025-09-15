package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// helper: clear DB dir and reopen
func resetTestDB(t testing.TB, path string) *StateDB {
	_ = os.RemoveAll(path)
	db, err := NewStateDB(path)
	if err != nil {
		t.Fatalf("NewStateDB failed: %v", err)
	}
	return db
}

// wrapper: run a batch in staging mode and return changes
func executeBatchStaged(b Batch, sm *SimpleStateMachine) (map[string][]byte, error) {
	// DB is not needed in staging mode → pass nil
	br, err := executeBatchWithDependencies(nil, b, sm, nil)
	if err != nil {
		return nil, err
	}
	return br.Changes, nil
}


// TestStagedDeterministicExecution simulates nodes processing batches with staging + merge
func TestStagedDeterministicExecution(t *testing.T) {
	dbPath := "./test_desync_staging"
	db := resetTestDB(t, dbPath)
	defer db.Close()

	// Create 100 batches × 100 txs = 10,000 txs
	totalTx := 10000
	txs := make([]Transaction, 0, totalTx)
	for i := 0; i < totalTx; i++ {
		data := []byte(fmt.Sprintf("k%d:%d", i, i*11))
		tx := Transaction{
			TxData:       data,
			TxID:         computeTxID(data),
			BlockHeight:  1,
			IndexInBlock: uint32(i),
		}
		txs = append(txs, tx)
	}
	ordered := orderTransactionsDeterministic(txs)

	activeWorkers := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		activeWorkers[i] = uint64(2000 + i)
	}

	prevRoot, _, _ := computeStateRootFromMap(map[string][]byte{})
	batches := createSequentialBatches(ordered, activeWorkers, 100, 1, prevRoot)
	if len(batches) != 100 {
		t.Fatalf("expected 100 batches, got %d", len(batches))
	}

	// --- Stage execution ---
	workerChanges := make([]map[string][]byte, len(batches))
	var wg sync.WaitGroup
	start := time.Now()

	for i, b := range batches {
		wg.Add(1)
		go func(idx int, batch Batch) {
			defer wg.Done()
			staging := make(map[string][]byte)
			sm := NewStagingStateMachine(staging)
			ch, err := executeBatchStaged(batch, sm)
			if err != nil {
				t.Errorf("worker %d error: %v", idx, err)
				return
			}
			workerChanges[idx] = ch
		}(i, b)
	}
	wg.Wait()

	// --- Deterministic merge ---
	merged := make(map[string][]byte)
	for _, ch := range workerChanges {
		for k, v := range ch {
			merged[k] = v
		}
	}
	if err := applyChangesToDB(db, merged); err != nil {
		t.Fatalf("applyChangesToDB failed: %v", err)
	}

	root, _ := computeStateRootFromDB(db)
	elapsed := time.Since(start)

	if len(root) == 0 {
		t.Fatalf("final root is empty")
	}

	if err := db.DumpToJSON("staging_state_dump.json"); err != nil {
		t.Fatalf("DumpToJSON failed: %v", err)
	}

	t.Logf("Staged deterministic execution: %d batches, %d txs, elapsed=%s, root=%s",
		len(batches), totalTx, elapsed, hex.EncodeToString(root))
}
// --- Benchmarks ---

var benchmarkBatches []Batch

// prepare shared batch data once
func prepareBenchmarkBatches(b *testing.B) {
	if len(benchmarkBatches) > 0 {
		return
	}
	totalTx := 10000
	txs := make([]Transaction, 0, totalTx)
	for i := 0; i < totalTx; i++ {
		data := []byte(fmt.Sprintf("bm_k%d:%d", i, i*17))
		tx := Transaction{
			TxData:       data,
			TxID:         computeTxID(data),
			BlockHeight:  1,
			IndexInBlock: uint32(i),
		}
		txs = append(txs, tx)
	}
	ordered := orderTransactionsDeterministic(txs)
	activeWorkers := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		activeWorkers[i] = uint64(5000 + i)
	}
	prevRoot, _, _ := computeStateRootFromMap(map[string][]byte{})
	benchmarkBatches = createSequentialBatches(ordered, activeWorkers, 100, 1, prevRoot)
}

// --- Accumulating DB ---
func BenchmarkStagedParallel_Accumulating(b *testing.B) {
	prepareBenchmarkBatches(b)
	dbPath := "./bench_desync_parallel_acc"
	db := resetTestDB(b, dbPath)
	defer db.Close()

	for n := 0; n < b.N; n++ {
		start := time.Now()
		workerChanges := make([]map[string][]byte, len(benchmarkBatches))
		var wg sync.WaitGroup
		for i, batch := range benchmarkBatches {
			wg.Add(1)
			go func(idx int, b Batch) {
				defer wg.Done()
				staging := make(map[string][]byte)
				sm := NewStagingStateMachine(staging)
				ch, _ := executeBatchStaged(b, sm)
				workerChanges[idx] = ch
			}(i, batch)
		}
		wg.Wait()

		merged := make(map[string][]byte)
		for _, ch := range workerChanges {
			for k, v := range ch {
				merged[k] = v
			}
		}
		_ = applyChangesToDB(db, merged)
		root, _ := computeStateRootFromDB(db)

		elapsed := time.Since(start)
		b.ReportMetric(float64(len(benchmarkBatches))/elapsed.Seconds(), "batches/s")
		b.ReportMetric(float64(10000)/elapsed.Seconds(), "tx/s")
		b.Logf("[Accum] iter=%d elapsed=%s root=%x", n, elapsed, root)
	}
}

// --- Resetting DB Each Iter ---
func BenchmarkStagedParallel_ResetEach(b *testing.B) {
	prepareBenchmarkBatches(b)

	for n := 0; n < b.N; n++ {
		dbPath := fmt.Sprintf("./bench_desync_parallel_reset_%d", n)
		db := resetTestDB(b, dbPath)

		start := time.Now()
		workerChanges := make([]map[string][]byte, len(benchmarkBatches))
		var wg sync.WaitGroup
		for i, batch := range benchmarkBatches {
			wg.Add(1)
			go func(idx int, b Batch) {
				defer wg.Done()
				staging := make(map[string][]byte)
				sm := NewStagingStateMachine(staging)
				ch, _ := executeBatchStaged(b, sm)
				workerChanges[idx] = ch
			}(i, batch)
		}
		wg.Wait()

		merged := make(map[string][]byte)
		for _, ch := range workerChanges {
			for k, v := range ch {
				merged[k] = v
			}
		}
		_ = applyChangesToDB(db, merged)
		root, _ := computeStateRootFromDB(db)

		elapsed := time.Since(start)
		b.ReportMetric(float64(len(benchmarkBatches))/elapsed.Seconds(), "batches/s")
		b.ReportMetric(float64(10000)/elapsed.Seconds(), "tx/s")
		b.Logf("[Reset] iter=%d elapsed=%s root=%x", n, elapsed, root)

		db.Close()
		_ = os.RemoveAll(dbPath) // cleanup after each iteration
	}
}
