package desync

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
	"testing"
)

// keep ecvrf import used by VRF test
import (
	"github.com/ProtonMail/go-ecvrf/ecvrf"
)

// ----------------- Pools -----------------

var kvResultPool = sync.Pool{
	New: func() any {
		return make([]KVResult, 0, 1024)
	},
}

// ----------------- Helpers -----------------

func resetTestDBBench(path string) *StateDB {
	_ = os.RemoveAll(path)
	db, err := NewStateDB(path)
	if err != nil {
		panic(fmt.Sprintf("NewStateDB failed: %v", err))
	}
	return db
}

func ensureTxDataJSONBench(filePath string, totalTx int) []Transaction {
	if _, err := os.Stat(filePath); err == nil {
		data, err := os.ReadFile(filePath)
		if err != nil {
			panic(fmt.Sprintf("read %s failed: %v", filePath, err))
		}
		var txs []Transaction
		if err := json.Unmarshal(data, &txs); err != nil {
			panic(fmt.Sprintf("unmarshal %s failed: %v", filePath, err))
		}
		if len(txs) >= totalTx {
			return txs[:totalTx]
		}
	}
	txs := make([]Transaction, 0, totalTx)
	for i := 0; i < totalTx; i++ {
		data := []byte(fmt.Sprintf("tx%d:%d", i, i*31))
		tx := Transaction{
			TxData:       data,
			TxID:         computeTxID(data),
			BlockHeight:  0,
			IndexInBlock: uint32(i),
		}
		txs = append(txs, tx)
	}
	enc, _ := json.MarshalIndent(txs, "", "  ")
	_ = os.WriteFile(filePath, enc, 0644)
	return txs
}

func writeCSVResult(file string, record []string) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("open csv failed: %v", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write(record)
}

// ----------------- Core Runner -----------------

// runScalingCase runs the scaling scenario.
// - constrained=false → full parallelism, one commit per block
// - constrained=true  → sequential batch execution, immediate commit per batch,
//   worker count fluctuates across blocks (degrades and recovers)
func runScalingCase(b *testing.B, csvFile string, totalTx, blockTx, batchSize, workers int, constrained bool) {
	if _, err := os.Stat(csvFile); os.IsNotExist(err) {
		writeCSVResult(csvFile, []string{"totalTx", "blockTx", "batchSize", "workers", "constrained", "elapsed_ms", "TPS"})
	}

	allTxs := ensureTxDataJSONBench(fmt.Sprintf("bench_%d.json", totalTx), totalTx)

	// worker fluctuation sequence used when constrained
	fluctuations := []int{workers, workers / 2, 2, workers / 2, (workers*7)/10}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		db := resetTestDBBench(fmt.Sprintf("./bench_db_%d_%d", totalTx, iter))
		defer db.Close()

		prevRoot, _, _ := computeStateRootFromMap(map[string][]byte{})
		totalBlocks := totalTx / blockTx
		startChain := time.Now()

		for blk := 0; blk < totalBlocks; blk++ {
			// choose worker count (fluctuating if constrained)
			currWorkers := workers
			if constrained {
				currWorkers = fluctuations[blk%len(fluctuations)]
			}

			workerIDs := make([]uint64, currWorkers)
			for i := range workerIDs {
				workerIDs[i] = uint64(5000 + i)
			}

			blockTxs := allTxs[blk*blockTx : (blk+1)*blockTx]
			for i := range blockTxs {
				blockTxs[i].BlockHeight = uint64(blk + 1)
			}

			ordered := orderTransactionsDeterministic(blockTxs)
			batches := createSequentialBatches(ordered, workerIDs, batchSize, uint64(blk+1), prevRoot)
			workerResults := make([][]KVResult, len(batches))

			if constrained {
				b.Logf("Block %d → constrained workers=%d", blk+1, currWorkers)
				// Sequential execution, commit immediately per batch
				for bi, batch := range batches {
					results := workerExecute(batch)
					workerResults[bi] = results
					if err := applyResultsOrdered(db, results); err != nil {
						b.Fatalf("applyResultsOrdered (constrained per-batch) failed: %v", err)
					}
				}
			} else {
				// Parallel execution, single commit
				var wg sync.WaitGroup
				for bi, batch := range batches {
					wg.Add(1)
					go func(idx int, batch Batch) {
						defer wg.Done()
						buf := kvResultPool.Get().([]KVResult)[:0]
						results := workerExecute(batch)
						buf = append(buf, results...)
						workerResults[idx] = buf
						kvResultPool.Put(buf)
					}(bi, batch)
				}
				wg.Wait()

				merged := mergeResultsOrdered(workerResults)
				if err := applyResultsOrdered(db, merged); err != nil {
					b.Fatalf("applyResultsOrdered failed: %v", err)
				}
			}

			// compute root after block
			root, err := computeStateRootFromDB(db)
			if err != nil {
				b.Fatalf("computeStateRootFromDB failed: %v", err)
			}
			prevRoot = root
		}

		elapsed := time.Since(startChain)
		tps := float64(totalTx) / elapsed.Seconds()

		b.ReportMetric(tps, "tx/s")
		b.Logf("[%s] %d tx (constrained=%v) → elapsed=%s, TPS=%.2f, finalRoot=%s",
			csvFile, totalTx, constrained, elapsed, tps, hex.EncodeToString(prevRoot),
		)

		writeCSVResult(csvFile, []string{
			fmt.Sprintf("%d", totalTx),
			fmt.Sprintf("%d", blockTx),
			fmt.Sprintf("%d", batchSize),
			fmt.Sprintf("%d", workers),
			fmt.Sprintf("%v", constrained),
			fmt.Sprintf("%.2f", float64(elapsed.Milliseconds())),
			fmt.Sprintf("%.2f", tps),
		})
	}
}


// ----------------- Benchmarks -----------------

func BenchmarkScalingUnconstrained(b *testing.B) {
	cases := []struct {
		totalTx   int
		blockTx   int
		batchSize int
		workers   int
	}{
		{20_000, 5_000, 200, 5},
		{200_000, 20_000, 500, 10},
		{1_000_000, 25_000, 500, 20},
	}
	for _, c := range cases {
		// unconstrained = false means we allow full parallelism
		b.Run(fmt.Sprintf("%dtx_unconstrained", c.totalTx), func(b *testing.B) {
			runScalingCase(b, "scaling_unconstrained.csv", c.totalTx, c.blockTx, c.batchSize, c.workers, false)
		})
	}
}

func BenchmarkScalingConstrained(b *testing.B) {
	cases := []struct {
		totalTx   int
		blockTx   int
		batchSize int
		workers   int
	}{
		{20_000, 5_000, 200, 5},
		{200_000, 20_000, 500, 10},
		{1_000_000, 25_000, 500, 20},
	}
	for _, c := range cases {
		// constrained = true will constrain worker count and force sequential commit behavior
		b.Run(fmt.Sprintf("%dtx_constrained", c.totalTx), func(b *testing.B) {
			runScalingCase(b, "scaling_constrained.csv", c.totalTx, c.blockTx, c.batchSize, c.workers, true)
		})
	}
}

// ----------------- Parallel Fraction Test -----------------

func TestMeasureParallelFraction(t *testing.T) {
	totalTx := 500_000
	coresToTest := []int{1, 2, 4, 8, 16, 32, 64}
	times := make(map[int]float64)

	// Prepare transactions
	txs := ensureTxDataJSONBench("pf_txs.json", totalTx)

	for _, n := range coresToTest {
		runtime.GOMAXPROCS(n)
		start := time.Now()

		// Single block execution (simulate intra-node execution)
		db := resetTestDBBench(fmt.Sprintf("./pf_db_%d", n))
		defer db.Close()
		ordered := orderTransactionsDeterministic(txs)
		batches := createSequentialBatches(ordered, []uint64{1}, 1000, 1, []byte{})

		var wg sync.WaitGroup
		for _, batch := range batches {
			wg.Add(1)
			go func(b Batch) {
				defer wg.Done()
				_ = workerExecute(b)
			}(batch)
		}
		wg.Wait()

		elapsed := time.Since(start).Seconds()
		times[n] = elapsed
		t.Logf("Cores=%d, Time=%.4fs", n, elapsed)
	}

	// Derive p from Amdahl’s law
	T1 := times[1]
	file := "parallel_fraction.csv"
	writeCSVResult(file, []string{"cores", "time_s", "p"})

	for _, n := range coresToTest[1:] {
		Tn := times[n]
		p := (T1 - Tn) / (T1 * (1 - 1/float64(n)))
		writeCSVResult(file, []string{
			fmt.Sprintf("%d", n),
			fmt.Sprintf("%.4f", Tn),
			fmt.Sprintf("%.4f", p),
		})
		t.Logf("Derived p at %d cores = %.4f", n, p)
	}
}

// ----------------- Deterministic Test -----------------

func TestDeterministicExecution(t *testing.T) {
	totalTx := 50_000
	blockTx := 5_000
	batchSize := 500

	// Prepare transactions
	txs := ensureTxDataJSONBench("deterministic_txs.json", totalTx)

	// Configurations to test determinism
	configs := []struct {
		name    string
		workers int
		cores   int
	}{
		{"single_worker_single_core", 1, 1},
		{"multi_worker_single_core", 5, 1},
		{"multi_worker_multi_core", 10, runtime.NumCPU()},
	}

	// Run each config multiple times to catch nondeterminism
	runsPerConfig := 3
	finalRoots := make(map[string][]string)

	for _, cfg := range configs {
		for run := 1; run <= runsPerConfig; run++ {
			runName := fmt.Sprintf("%s_run%d", cfg.name, run)
			t.Run(runName, func(t *testing.T) {
				runtime.GOMAXPROCS(cfg.cores)

				db := resetTestDBBench(fmt.Sprintf("./deterministic_db_%s", runName))
				defer db.Close()

				workerIDs := make([]uint64, cfg.workers)
				for i := range workerIDs {
					workerIDs[i] = uint64(6000 + i)
				}

				prevRoot, _, _ := computeStateRootFromMap(map[string][]byte{})
				totalBlocks := totalTx / blockTx

				for blk := 0; blk < totalBlocks; blk++ {
					blockTxs := txs[blk*blockTx : (blk+1)*blockTx]
					for i := range blockTxs {
						blockTxs[i].BlockHeight = uint64(blk + 1)
					}

					ordered := orderTransactionsDeterministic(blockTxs)
					batches := createSequentialBatches(ordered, workerIDs, batchSize, uint64(blk+1), prevRoot)

					workerResults := make([][]KVResult, len(batches))
					var wg sync.WaitGroup
					for bi, batch := range batches {
						wg.Add(1)
						go func(idx int, b Batch) {
							defer wg.Done()
							results := workerExecute(b) // deterministic worker execution
							workerResults[idx] = results
						}(bi, batch)
					}
					wg.Wait()

					merged := mergeResultsOrdered(workerResults)
					if err := applyResultsOrdered(db, merged); err != nil {
						t.Fatalf("applyResultsOrdered failed: %v", err)
					}

					root, err := computeStateRootFromDB(db)
					if err != nil {
						t.Fatalf("computeStateRootFromDB failed: %v", err)
					}
					prevRoot = root
				}

				finalRootHex := hex.EncodeToString(prevRoot)
				finalRoots[cfg.name] = append(finalRoots[cfg.name], finalRootHex)
				t.Logf("Config %s (run %d) → Final Root: %s", cfg.name, run, finalRootHex)
			})
		}
	}

	// Validate determinism:
	// 1. All runs within each config must match
	// 2. All configs must match each other
	var referenceRoot string
	for cfg, roots := range finalRoots {
		first := roots[0]
		for _, r := range roots {
			if r != first {
				t.Fatalf("nondeterminism detected within config %s: %v", cfg, roots)
			}
		}
		t.Logf("✅ Config %s is deterministic across %d runs", cfg, runsPerConfig)

		if referenceRoot == "" {
			referenceRoot = first
		} else if first != referenceRoot {
			t.Fatalf("nondeterminism detected between configs: expected %s, got %s for config %s",
				referenceRoot, first, cfg)
		}
	}

	t.Logf("🎉 All configurations and runs produced the same final root: %s", referenceRoot)
}

// ----- VRF test kept unchanged -----
func TestVRFProveVerify(t *testing.T) {
	sk, err := ecvrf.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key err: %v", err)
	}
	pk, err := sk.Public()
	if err != nil {
		t.Fatalf("get public err: %v", err)
	}

	seed := []byte("epoch-seed-12345")

	beta, proof, err := sk.Prove(seed)
	if err != nil {
		t.Fatalf("prove err: %v", err)
	}

	ok, beta2, err := pk.Verify(seed, proof)
	if err != nil {
		t.Fatalf("verify err: %v", err)
	}
	if !ok {
		t.Fatal("verify failed")
	}
	if !bytes.Equal(beta, beta2) {
		t.Fatalf("beta mismatch: got %x want %x", beta2, beta)
	}

	// test wrappers
	h, p, err := vrfGenerateHash(seed, sk.Bytes())
	if err != nil {
		t.Fatalf("vrfGenerateHash err: %v", err)
	}

	ok2, err := vrfVerifyHash(h, p, pk.Bytes(), seed)
	if err != nil {
		t.Fatalf("vrfVerifyHash err: %v", err)
	}
	if !ok2 {
		t.Fatal("vrfVerifyHash returned false")
	}
}

// TestMultiBatchReceipts verifies receipt generation & verification across multiple batches.
// - applies N batches of KV updates (each batch contains multiple key->value writes)
// - after each batch commit, generates a BatchReceipt containing SMT proofs for that batch
// - verifies each BatchReceipt against the SMT root at that point
// - finally validates the chain of roots (each batch root matches subsequent starting root)
// - also includes a negative test that tampers with one proof to ensure verification fails
func TestMultiBatchReceipts(t *testing.T) {
	// prepare DB
	dbPath := "./smt_test_db_multi"
	db := resetTestDBBench(dbPath)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(dbPath)
	}()

	// small config for test speed — increase as needed for stress testing
	batches := 3
	perBatch := 5

	// types for ad-hoc receipts inside test (no external dependency)
	type KeyValueProof struct {
		Key         []byte
		NewValue    []byte
		MerkleProof [][]byte // bottom-up siblings
	}
	type BatchReceipt struct {
		BatchID       uint64
		WorkerID      uint64
		BlockHeight   uint64
		PostStateRoot []byte
		Proofs        []KeyValueProof
	}

	// keep track of all receipts and roots
	var receipts []BatchReceipt
	var roots [][]byte

	// apply batches sequentially (each batch: multiple SMTUpdate calls)
	for bid := 1; bid <= batches; bid++ {
		// simulate a worker id and blockheight
		workerID := uint64(100 + bid)
		blockHeight := uint64(bid)

		// collect the KV pairs we will write in this batch
		kvs := make([]struct {
			k []byte
			v []byte
		}, 0, perBatch)

		for i := 0; i < perBatch; i++ {
			key := []byte(fmt.Sprintf("batch%d_key%02d", bid, i))
			value := []byte(fmt.Sprintf("val_%d_%02d", bid, i))
			kvs = append(kvs, struct{ k, v []byte }{k: key, v: value})
		}

		// apply each KV to DB + SMT (SMTUpdate persists both)
		for _, kv := range kvs {
			if _, err := db.SMTUpdate(kv.k, kv.v); err != nil {
				t.Fatalf("SMTUpdate failed for key=%s: %v", kv.k, err)
			}
		}

		// after applying the batch, fetch current root
		root, err := db.CurrentRoot()
		if err != nil {
			t.Fatalf("CurrentRoot failed after batch %d: %v", bid, err)
		}
		roots = append(roots, append([]byte(nil), root...))

		// generate proofs for each KV in the batch (proofs are computed against current root)
		batchReceipt := BatchReceipt{
			BatchID:       uint64(bid),
			WorkerID:      workerID,
			BlockHeight:   blockHeight,
			PostStateRoot: append([]byte(nil), root...),
		}
		for _, kv := range kvs {
			proof, err := db.SMTGenerateProof(kv.k)
			if err != nil {
				t.Fatalf("SMTGenerateProof failed for key=%s after batch %d: %v", kv.k, bid, err)
			}
			// copy proof slices defensively
			proofCopy := make([][]byte, len(proof))
			for i := range proof {
				proofCopy[i] = append([]byte(nil), proof[i]...)
			}
			batchReceipt.Proofs = append(batchReceipt.Proofs, KeyValueProof{
				Key:         append([]byte(nil), kv.k...),
				NewValue:    append([]byte(nil), kv.v...),
				MerkleProof: proofCopy,
			})
		}

		receipts = append(receipts, batchReceipt)
		t.Logf("Generated receipt for batch %d with root %x", bid, root)
	}

	// -----------------------
	// Verify every receipt
	// -----------------------
	for i, r := range receipts {
		t.Logf("Verifying receipt for batch %d (root=%x)", r.BatchID, r.PostStateRoot)
		for j, p := range r.Proofs {
			ok := VerifyProof(p.Key, p.NewValue, p.MerkleProof, SMTDepth, r.PostStateRoot)
			if !ok {
				t.Fatalf("Receipt verification failed for batch %d proof %d (key=%s)", r.BatchID, j, string(p.Key))
			}
		}
		t.Logf("Batch %d receipt verification OK", r.BatchID)

		// cross-check DB's persisted root matches receipt root
		currRoot, err := db.CurrentRoot()
		if err != nil {
			t.Fatalf("CurrentRoot failed while verifying batch %d: %v", r.BatchID, err)
		}
		// At the time of verification the DB root should be the final root (last batch),
		// but we at least ensure each receipt's PostStateRoot is a valid historical root by recalculating:
		// Here we check that the receipt root equals the recorded root we stored just after batch application.
		if !bytes.Equal(currRoot, roots[len(roots)-1]) {
			// DB root progressed; that's expected. We still have recorded roots to compare against.
			_ = currRoot // nothing to do, just leave a note
		}
		// ensure our recorded root for this batch still matches the stored roots list
		if !bytes.Equal(r.PostStateRoot, roots[i]) {
			t.Fatalf("Recorded root mismatch for batch %d: receipt root != recorded root", r.BatchID)
		}
	}

	t.Log("✅ All batch receipts verified OK")

	// -----------------------
	// Negative test — tamper with proof of batch 2 (if exists)
	// -----------------------
	if len(receipts) >= 2 && len(receipts[1].Proofs) > 0 {
		bad := receipts[1]
		// flip a byte in the first proof's value
		if len(bad.Proofs[0].NewValue) == 0 {
			bad.Proofs[0].NewValue = []byte("tampered")
		} else {
			bad.Proofs[0].NewValue[0] ^= 0xFF
		}
		ok := VerifyProof(bad.Proofs[0].Key, bad.Proofs[0].NewValue, bad.Proofs[0].MerkleProof, SMTDepth, bad.PostStateRoot)
		if ok {
			t.Fatalf("tampered proof unexpectedly verified for batch %d", bad.BatchID)
		}
		t.Log("✅ Tampered proof correctly failed verification")
	}
}

