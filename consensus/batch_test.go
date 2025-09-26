package consensus

import (
	"crypto/ed25519"
	//"time"

	"github.com/MelzLabs/DeSync/types"
)

// helper: create a signed tx
func makeSigned(sender string, priv ed25519.PrivateKey, nonce uint64, action string, payload map[string]string) *types.Transaction {
	tx := types.NewTxn(sender, action, payload)
	tx.Nonce = nonce
	tx.Sign(priv)
	return tx
}

// func TestPartitionBatch(t *testing.T) {
// 	// Create some test transactions
// 	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	
// 	txs := []*types.Transaction{
// 		makeSigned("alice", priv, 1, "Grow", map[string]string{"plotID": "p1", "amount": "1"}),
// 		makeSigned("bob", priv, 1, "Grow", map[string]string{"plotID": "p2", "amount": "1"}),
// 		makeSigned("alice", priv, 2, "Grow", map[string]string{"plotID": "p1", "amount": "2"}),
// 		makeSigned("charlie", priv, 1, "Transfer", map[string]string{"from": "charlie", "to": "dave", "amount": "10"}),
// 	}
	
// 	shards := partitionBatch(txs)
	
// 	if len(shards) < 2 {
// 		t.Fatalf("Expected at least 2 shards due to conflicts, got %d", len(shards))
// 	}
	
// 	// Verify that conflicting transactions are in different shards
// 	// tx[0] and tx[2] both touch plotID p1, so they should be in different shards
// 	found0, found2 := -1, -1
// 	for i, shard := range shards {
// 		for _, tx := range shard {
// 			if tx.ID == txs[0].ID {
// 				found0 = i
// 			}
// 			if tx.ID == txs[2].ID {
// 				found2 = i
// 			}
// 		}
// 	}
	
// 	if found0 == found2 {
// 		t.Errorf("Conflicting transactions should be in different shards, both in shard %d", found0)
// 	}
// }

// Stress test: submit 10k txs and measure throughput
// Uncomment and modify as needed for testing
/*
func TestStressBatch10000(t *testing.T) {
	const totalTx = 10000
	const batchSize = 500
	const workers = 8

	// set up leader + smt
	smt := state.NewSparseMerkleTree()
	leader := NewLeader("leader-stress", workers, smt)

	// generate keypair for alice
	_, privA, _ := ed25519.GenerateKey(rand.Reader)

	// init alice with high balance
	leader.StateInit("alice", 1000000)

	// submit txs in a goroutine so BatchAndExecute can consume concurrently
	submitDone := make(chan struct{})
	go func() {
		for i := 0; i < totalTx; i++ {
			tx := makeSigned("alice", privA, uint64(i), "Grow",
				map[string]string{"plotID": "p1", "amount": "1"})
			if err := leader.SubmitTx(tx); err != nil {
				t.Fatalf("submit tx err: %v", err)
			}
		}
		close(submitDone)
	}()

	// consume batches until we've processed all txs
	start := time.Now()
	processed := 0
	for processed < totalTx {
		rec, err := leader.BatchAndExecute(batchSize, 200*time.Millisecond)
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		for _, r := range rec.TxResults {
			if r != nil && r.Success {
				processed++
			}
		}
		if processed%2500 == 0 {
			fmt.Printf("processed %d/%d\n", processed, totalTx)
		}
	}
	elapsed := time.Since(start)
	t.Logf("Processed %d txs in %s (workers=%d)", totalTx, elapsed, workers)

	// final check: last-write-wins means value is nondeterministic.
	// Just ensure a valid proof exists for the final stored value.
	proof, err := smt.Prove([]byte("land:p1:resource"))
	if err != nil {
		t.Fatalf("prove err: %v", err)
	}

	snap := smt.ExportSnapshot()
	val, ok := snap["land:p1:resource"]
	if !ok {
		t.Fatalf("resource key missing from snapshot")
	}

	if !state.VerifyProof(proof, smt.GetRoot(), []byte("land:p1:resource"), val) {
		t.Fatalf("final proof failed: root/proof mismatch for resource")
	}
}
*/