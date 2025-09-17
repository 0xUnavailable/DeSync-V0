package desync

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"testing"

	"github.com/ProtonMail/go-ecvrf/ecvrf"
)

type ElectionResult struct {
	NodeID int
	Beta   []byte
	Proof  []byte
	Pub    *ecvrf.PublicKey
}

// Simulate 10 worker nodes competing in leader election
func TestConcurrentLeaderElection(t *testing.T) {
	const numWorkers = 10
	seed := []byte("epoch-seed-99999")

	resultsCh := make(chan ElectionResult, numWorkers)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Generate VRF keypair
			sk, err := ecvrf.GenerateKey(rand.Reader)
			if err != nil {
				t.Errorf("node %d: keygen failed: %v", id, err)
				return
			}
			pk, err := sk.Public()
			if err != nil {
				t.Errorf("node %d: pubkey failed: %v", id, err)
				return
			}

			// Prove
			beta, proof, err := sk.Prove(seed)
			if err != nil {
				t.Errorf("node %d: prove failed: %v", id, err)
				return
			}

			// Send result back to coordinator
			resultsCh <- ElectionResult{
				NodeID: id,
				Beta:   beta,
				Proof:  proof,
				Pub:    pk,
			}
		}(i)
	}

	// Close resultsCh after all goroutines finish
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Coordinator: collect + verify results
	var results []ElectionResult
	for res := range resultsCh {
		ok, beta2, err := res.Pub.Verify(seed, res.Proof)
		if err != nil {
			t.Fatalf("node %d: verification error: %v", res.NodeID, err)
		}
		if !ok || !bytes.Equal(beta2, res.Beta) {
			t.Fatalf("node %d: VRF verification failed", res.NodeID)
		}
		results = append(results, res)
	}

	if len(results) != numWorkers {
		t.Fatalf("expected %d results, got %d", numWorkers, len(results))
	}

	// Elect leader = lowest beta
	sort.Slice(results, func(i, j int) bool {
		ival := new(big.Int).SetBytes(results[i].Beta)
		jval := new(big.Int).SetBytes(results[j].Beta)
		return ival.Cmp(jval) < 0
	})

	leader := results[0]
	fmt.Printf("✅ Leader elected: Node %d with VRF output %s\n",
		leader.NodeID, hex.EncodeToString(leader.Beta[:8]))
}
