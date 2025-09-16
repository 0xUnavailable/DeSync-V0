package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {
	fmt.Println("DESYNC prototype (staging writes). Demo run...")

	dbPath := "./desync_db"

	var err error
	stateDB, err = NewStateDB(dbPath)
	if err != nil {
		log.Fatalf("failed to open state DB: %v", err)
	}
	defer stateDB.Close()

	ps, err := stateDB.LoadPersistentState()
	if err != nil {
		ps = &PersistentState{
			CurrentEpoch: 1,
			NodeID:       1,
			PrivateKey:   []byte("demo-private-key-32bytes-length-should-be-replaced"),
			Stake:        1000,
			StateRoot:    []byte{},
		}
		if err := stateDB.SavePersistentState(ps); err != nil {
			log.Fatalf("failed to save persistent_state: %v", err)
		}
	}
	fmt.Println("restored epoch:", ps.CurrentEpoch)

	// create fake transactions in received order (IndexInBlock preserves order)
	txs := []Transaction{}
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("key%d:%d", i, i*10))
		tx := Transaction{
			TxData:       data,
			TxID:         computeTxID(data),
			BlockHeight:  100,
			IndexInBlock: uint32(i),
		}
		txs = append(txs, tx)
	}
	ordered := orderTransactionsDeterministic(txs)

	prevRoot, err := computeStateRootFromDB(stateDB)
	if err != nil {
		prevRoot = []byte{}
	}

	activeWorkers := []uint64{101, 102}
	batches := createSequentialBatches(ordered, activeWorkers, 4, 1, prevRoot)
	fmt.Printf("Created %d batches\n", len(batches))

	workerResults := make([][]KVResult, len(batches))
	var wg sync.WaitGroup
	start := time.Now()

	for i, b := range batches {
		wg.Add(1)
		go func(idx int, batch Batch) {
			defer wg.Done()
			workerResults[idx] = workerExecute(batch) // replace with intraNodeExecute(batch) to use multi-core inside worker
		}(i, b)
	}
	wg.Wait()

	merged := mergeResultsOrdered(workerResults)

	if err := applyResultsOrdered(stateDB, merged); err != nil {
		log.Fatalf("applyResultsOrdered failed: %v", err)
	}

	root, _ := computeStateRootFromDB(stateDB)
	elapsed := time.Since(start)
	fmt.Printf("Final root after merged commit: %x (elapsed %s)\n", root, elapsed)

	ps.StateRoot = root
	_ = stateDB.SavePersistentState(ps)
}
