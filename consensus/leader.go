package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/MelzLabs/DeSync/network"
	"github.com/MelzLabs/DeSync/state"
	"github.com/MelzLabs/DeSync/types"
)

// Leader is a single static leader (no rotation).
type Leader struct {
	ID      string
	NetNode *network.Node

	// authoritative state
	State *state.MemStore

	// tx queue
	txQueueMu sync.Mutex
	txQueue   []*types.Transaction

	// pending batches
	pendingMu           sync.Mutex
	pendingBatchIDs     []string
	pendingBatchTxs     map[string][]*types.Transaction
	nextBatchSequential int

	// simple block height
	blockMu sync.Mutex
	height  int
}

func NewLeader(id string) *Leader {
	return &Leader{
		ID:                  id,
		State:               state.NewMemStore(),
		txQueue:             make([]*types.Transaction, 0, 1024),
		pendingBatchTxs:     make(map[string][]*types.Transaction),
		pendingBatchIDs:     make([]string, 0),
		nextBatchSequential: 0,
	}
}

// === Worker registry ===
func (l *Leader) StartWorkerRegistry(ctx context.Context) error {
	sub, err := l.NetNode.WorkersTopic.Subscribe()
	if err != nil {
		return err
	}
	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			wh, err := network.DecodeWorkerHello(msg.Data)
			if err != nil {
				continue
			}
			log.Printf("Leader %s: observed worker %s (peer=%s)", l.ID, wh.WorkerID, wh.PeerID)
		}
	}()
	return nil
}

// === Tx ingestion ===
func (l *Leader) StartTxListener(ctx context.Context) error {
	sub, err := l.NetNode.TxTopic.Subscribe()
	if err != nil {
		return err
	}
	log.Printf("Leader %s: subscribed to TxTopic", l.ID)

	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			txm, err := network.DecodeTx(msg.Data)
			if err != nil {
				continue
			}
			tx := txm.Tx
			if tx == nil {
				continue
			}
			if !tx.VerifySignature() {
				continue
			}
			l.enqueueTx(tx)
		}
	}()
	return nil
}

func (l *Leader) enqueueTx(tx *types.Transaction) {
	l.txQueueMu.Lock()
	l.txQueue = append(l.txQueue, tx)
	l.txQueueMu.Unlock()
}

func (l *Leader) dequeueBatch(max int) []*types.Transaction {
	l.txQueueMu.Lock()
	defer l.txQueueMu.Unlock()
	if len(l.txQueue) == 0 {
		return nil
	}
	n := max
	if len(l.txQueue) < n {
		n = len(l.txQueue)
	}
	batch := l.txQueue[:n]
	l.txQueue = l.txQueue[n:]
	return batch
}

// === Batch loop ===
func (l *Leader) RunBatchLoop(ctx context.Context, batchSize int, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("Leader %s: starting batch loop (batchSize=%d interval=%s)", l.ID, batchSize, interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			txs := l.dequeueBatch(batchSize)
			if len(txs) == 0 {
				continue
			}
			batchID := fmt.Sprintf("batch-%d", l.nextBatchSequential)
			l.nextBatchSequential++

			l.pendingMu.Lock()
			l.pendingBatchTxs[batchID] = txs
			l.pendingBatchIDs = append(l.pendingBatchIDs, batchID)
			l.pendingMu.Unlock()

			snapshot := l.State.GetSnapshot()

			bm := network.BatchMessage{
				BatchID:      batchID,
				Txs:          txs,
				ShardIndex:   0,
				ShardCount:   1,
				TargetWorker: "",
				Snapshot:     snapshot,
			}
			data, _ := network.Encode(bm)
			if err := l.NetNode.BatchTopic.Publish(ctx, data); err != nil {
				log.Printf("Leader %s: publish batch error: %v", l.ID, err)
			} else {
				log.Printf("Leader %s: published batch %s (txs=%d snapshot_keys=%d)", l.ID, batchID, len(txs), len(snapshot))
			}
		}
	}
}

// === Results collector ===
func (l *Leader) ListenResults(ctx context.Context) error {
	sub, err := l.NetNode.ResultTopic.Subscribe()
	if err != nil {
		return err
	}

	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			sr, err := network.DecodeResult(msg.Data)
			if err != nil {
				continue
			}

			applied := 0
			failed := 0
			for _, r := range sr.Results {
				if r == nil {
					continue
				}
				if r.Success {
					for k, v := range r.WriteSet {
						l.State.Set([]byte(k), v)
						applied++
					}
				} else {
					failed++
				}
			}

			log.Printf("Leader %s: results from worker=%s batch=%s applied_writes=%d failures=%d",
				l.ID, sr.WorkerID, sr.BatchID, applied, failed)
		}
	}()

	return nil
}

// === Queue monitor ===
func (l *Leader) StartQueueMonitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.txQueueMu.Lock()
			q := len(l.txQueue)
			l.txQueueMu.Unlock()
			if q > 0 {
				log.Printf("Leader %s: queue length=%d", l.ID, q)
			}
		}
	}
}

// === Block ticker ===
func (l *Leader) BlockTicker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	blocksDir := "blocks"
	_ = os.MkdirAll(blocksDir, 0755)
	indexFile := filepath.Join(blocksDir, "index.json")

	// load existing index if any
	var index []map[string]interface{}
	if data, err := os.ReadFile(indexFile); err == nil {
		_ = json.Unmarshal(data, &index)
	}

	log.Printf("Leader %s: starting block ticker (interval=%s)", l.ID, interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.blockMu.Lock()
			l.height++
			height := l.height
			l.blockMu.Unlock()

			l.pendingMu.Lock()
			pendingIDs := l.pendingBatchIDs
			batchesMap := l.pendingBatchTxs
			l.pendingBatchIDs = nil
			l.pendingBatchTxs = make(map[string][]*types.Transaction)
			l.pendingMu.Unlock()

			collected := make([]*types.Transaction, 0)
			for _, bid := range pendingIDs {
				if txs, ok := batchesMap[bid]; ok {
					collected = append(collected, txs...)
				}
			}

			if len(collected) == 0 {
				continue
			}

			blk := types.Block{
				Height: height,
				Txs:    collected,
				Root:   []byte("dummy-root"),
				Time:   time.Now(),
			}

			filename := fmt.Sprintf("block-%06d.json", blk.Height)
			fullPath := filepath.Join(blocksDir, filename)
			data, _ := json.MarshalIndent(blk, "", "  ")
			if err := os.WriteFile(fullPath, data, 0644); err != nil {
				log.Printf("Leader %s: failed to save block #%d: %v", l.ID, blk.Height, err)
				continue
			}

			entry := map[string]interface{}{
				"height":   blk.Height,
				"filename": filename,
				"txs":      len(collected),
				"time":     blk.Time,
			}
			index = append(index, entry)
			indexData, _ := json.MarshalIndent(index, "", "  ")
			_ = os.WriteFile(indexFile, indexData, 0644)

			log.Printf("Leader %s: committed block #%d (txs=%d)", l.ID, blk.Height, len(collected))
		}
	}
}
