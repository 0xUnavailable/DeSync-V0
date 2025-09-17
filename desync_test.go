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
	"testing"
	"time"

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

func runScalingCase(b *testing.B, csvFile string, totalTx, blockTx, batchSize, workers int) {
	if _, err := os.Stat(csvFile); os.IsNotExist(err) {
		writeCSVResult(csvFile, []string{"totalTx", "blockTx", "batchSize", "workers", "elapsed_ms", "TPS"})
	}

	allTxs := ensureTxDataJSONBench(fmt.Sprintf("bench_%d.json", totalTx), totalTx)
	workerIDs := make([]uint64, workers)
	for i := range workerIDs {
		workerIDs[i] = uint64(5000 + i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db := resetTestDBBench(fmt.Sprintf("./bench_db_%d", totalTx))
		defer db.Close()

		prevRoot, _ := db.CurrentRoot()
		totalBlocks := totalTx / blockTx
		startChain := time.Now()

		for blk := 0; blk < totalBlocks; blk++ {
			blockTxs := allTxs[blk*blockTx : (blk+1)*blockTx]
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
					results := workerExecute(b)
					workerResults[idx] = results
				}(bi, batch)
			}
			wg.Wait()

			merged := mergeResultsOrdered(workerResults)
			if err := applyResultsOrdered(db, merged); err != nil {
				b.Fatalf("applyResultsOrdered failed: %v", err)
			}

			root, err := db.CurrentRoot()
			if err != nil {
				b.Fatalf("db.CurrentRoot failed: %v", err)
			}
			prevRoot = root
		}

		elapsed := time.Since(startChain)
		tps := float64(totalTx) / elapsed.Seconds()

		b.ReportMetric(tps, "tx/s")
		b.Logf("[%s] %d tx → elapsed=%s, TPS=%.2f, finalRoot=%s",
			csvFile, totalTx, elapsed, tps, hex.EncodeToString(prevRoot),
		)

		writeCSVResult(csvFile, []string{
			fmt.Sprintf("%d", totalTx),
			fmt.Sprintf("%d", blockTx),
			fmt.Sprintf("%d", batchSize),
			fmt.Sprintf("%d", workers),
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
		b.Run(fmt.Sprintf("%dtx_unconstrained", c.totalTx), func(b *testing.B) {
			runScalingCase(b, "scaling_unconstrained.csv", c.totalTx, c.blockTx, c.batchSize, c.workers)
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
		b.Run(fmt.Sprintf("%dtx_constrained", c.totalTx), func(b *testing.B) {
			runScalingCase(b, "scaling_constrained.csv", c.totalTx, c.blockTx, c.batchSize, c.workers)
		})
	}
}

// ----------------- Parallel Fraction Test -----------------

func TestMeasureParallelFraction(t *testing.T) {
	totalTx := 200_000
	coresToTest := []int{1, 2, 4, 8, 16}
	times := make(map[int]float64)

	// Prepare transactions
	txs := ensureTxDataJSONBench("pf_txs.json", totalTx)

	for _, n := range coresToTest {
		runtime.GOMAXPROCS(n)
		start := time.Now()

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

	txs := ensureTxDataJSONBench("deterministic_txs.json", totalTx)

	configs := []struct {
		name    string
		workers int
		cores   int
	}{
		{"single_worker_single_core", 1, 1},
		{"multi_worker_single_core", 5, 1},
		{"multi_worker_multi_core", 10, runtime.NumCPU()},
	}

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

				prevRoot, _ := db.CurrentRoot()
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
							results := workerExecute(b)
							workerResults[idx] = results
						}(bi, batch)
					}
					wg.Wait()

					merged := mergeResultsOrdered(workerResults)
					if err := applyResultsOrdered(db, merged); err != nil {
						t.Fatalf("applyResultsOrdered failed: %v", err)
					}

					root, err := db.CurrentRoot()
					if err != nil {
						t.Fatalf("db.CurrentRoot failed: %v", err)
					}
					prevRoot = root
				}

				finalRootHex := hex.EncodeToString(prevRoot)
				finalRoots[cfg.name] = append(finalRoots[cfg.name], finalRootHex)
				t.Logf("Config %s (run %d) → Final Root: %s", cfg.name, run, finalRootHex)
			})
		}
	}

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

// ----------------- VRF Test -----------------

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

	// wrapper functions
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

// ----------------- SMT Proof Test -----------------

func TestSMTProofRoundTrip(t *testing.T) {
	db := resetTestDBBench("./smt_test_db")
	defer db.Close()

	key := []byte("foo")
	value := []byte("bar")

	// Update state
	root, err := db.SMTUpdate(key, value)
	if err != nil {
		t.Fatalf("SMTUpdate err: %v", err)
	}

	// Generate proof
	proof, err := db.SMTGenerateProof(key)
	if err != nil {
		t.Fatalf("SMTGenerateProof err: %v", err)
	}

	// Verify proof
	ok := VerifyProof(key, value, proof, SMTDepth, root)
	if !ok {
		t.Fatalf("VerifyProof failed: key=%s value=%s root=%x", key, value, root)
	}

	t.Logf("✅ SMT proof verified successfully for key=%s value=%s root=%x", key, value, root)
}
