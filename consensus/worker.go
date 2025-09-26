package consensus

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MelzLabs/DeSync/execution"
	"github.com/MelzLabs/DeSync/network"
	"github.com/MelzLabs/DeSync/types"
)

// Worker executes batches, verifies tx signatures and publishes results.
type Worker struct {
	ID      string
	NetNode *network.Node

	privKey ed25519.PrivateKey
	pubKey  ed25519.PublicKey

	totalProcessed int64
	totalShards    int64

	processedMu sync.Mutex
	processed   map[string]struct{}
}

func NewWorker(id string, node *network.Node) *Worker {
	return &Worker{
		ID:        id,
		NetNode:   node,
		processed: make(map[string]struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) error {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	w.privKey = priv
	w.pubKey = priv.Public().(ed25519.PublicKey)

	// initial hello (non-blocking)
	wh := network.WorkerHello{
		WorkerID: w.ID,
		PubKey:   []byte(w.pubKey),
		PeerID:   w.NetNode.Host.ID().String(),
	}
	if data, err := network.EncodeWorkerHello(wh); err == nil {
		_ = w.NetNode.WorkersTopic.Publish(ctx, data)
	}

	// subscribe to leader heartbeats (just to log, passive)
	if sub, err := w.NetNode.LeaderTopic.Subscribe(); err == nil {
		go func() {
			for {
				msg, err := sub.Next(ctx)
				if err != nil {
					return
				}
				ann, err := network.DecodeLeaderAnnounce(msg.Data)
				if err != nil {
					continue
				}
				log.Printf("Worker %s: leader heartbeat from %s (epoch=%d)", w.ID, ann.LeaderID, ann.Epoch)
			}
		}()
	}

	// performance monitor
	go w.startPerformanceMonitor(ctx)

	// subscribe to batch topic
	sub, err := w.NetNode.BatchTopic.Subscribe()
	if err != nil {
		return err
	}
	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			bm, err := network.DecodeBatch(msg.Data)
			if err != nil {
				continue
			}
			// if the batch targeted a specific worker, skip if not me
			if bm.TargetWorker != "" && bm.TargetWorker != w.ID {
				continue
			}
			go w.processShard(ctx, bm)
		}
	}()

	return nil
}

func (w *Worker) startPerformanceMonitor(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processed := atomic.LoadInt64(&w.totalProcessed)
			shards := atomic.LoadInt64(&w.totalShards)
			if shards > 0 {
				avg := float64(processed) / float64(shards)
				log.Printf("Worker %s STATS: processed=%d txs, shards=%d, avg=%.2f",
					w.ID, processed, shards, avg)
			}
		}
	}
}

func (w *Worker) processShard(ctx context.Context, bm *network.BatchMessage) {
	key := bm.BatchID + "-" + string(rune(bm.ShardIndex))
	w.processedMu.Lock()
	if _, seen := w.processed[key]; seen {
		w.processedMu.Unlock()
		return
	}
	w.processed[key] = struct{}{}
	w.processedMu.Unlock()

	start := time.Now()
	atomic.AddInt64(&w.totalShards, 1)

	// create sandbox state from snapshot (may be nil)
	snap := execution.NewSandboxedState(bm.Snapshot)
	results := make([]*types.TxResult, len(bm.Txs))

	var wg sync.WaitGroup
	wg.Add(len(bm.Txs))
	for i, tx := range bm.Txs {
		i, tx := i, tx
		go func() {
			defer wg.Done()
			// verify tx signature
			if !tx.VerifySignature() {
				results[i] = &types.TxResult{
					TxID:    tx.ID,
					Index:   i,
					Success: false,
					Error:   "invalid signature",
				}
				return
			}
			// execute
			results[i] = execution.ApplyTransaction(snap, tx, w.ID, i)
		}()
	}
	wg.Wait()
	atomic.AddInt64(&w.totalProcessed, int64(len(bm.Txs)))
	elapsed := time.Since(start)

	// --- Success/fail counters + failure reason breakdown ---
	successCount := 0
	failCount := 0
	errorReasons := make(map[string]int)

	for _, r := range results {
		if r != nil && r.Success {
			successCount++
		} else if r != nil {
			failCount++
			reason := r.Error
			if reason == "" {
				reason = "unknown"
			}
			errorReasons[reason]++
		}
	}

	log.Printf("Worker %s: finished shard %d/%d batch %s (txs=%d success=%d fail=%d elapsed=%s)",
		w.ID, bm.ShardIndex+1, bm.ShardCount, bm.BatchID,
		len(bm.Txs), successCount, failCount, elapsed)

	if failCount > 0 {
		for reason, count := range errorReasons {
			log.Printf("  └─ fail_reason=%q count=%d", reason, count)
		}
	}

	// sign results blob
	toSign := struct {
		BatchID    string
		WorkerID   string
		ShardIndex int
		ShardCount int
		Results    []*types.TxResult
	}{
		BatchID:    bm.BatchID,
		WorkerID:   w.ID,
		ShardIndex: bm.ShardIndex,
		ShardCount: bm.ShardCount,
		Results:    results,
	}
	payload, _ := json.Marshal(toSign)
	sig := ed25519.Sign(w.privKey, payload)

	sr := network.SignedResult{
		BatchID:    bm.BatchID,
		WorkerID:   w.ID,
		ShardIndex: bm.ShardIndex,
		ShardCount: bm.ShardCount,
		Results:    results,
		PubKey:     []byte(w.pubKey),
		Sig:        sig,
	}
	data, _ := network.EncodeResult(sr)
	_ = w.NetNode.ResultTopic.Publish(ctx, data)
}
