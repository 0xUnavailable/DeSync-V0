package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// InitWorld seeds the in-memory state with a size x size grid of plots.
// It is idempotent: it will not overwrite existing owner/resource/quota keys.
func (l *Leader) InitWorld(size int) {
	if size <= 0 {
		return
	}
	
	// Initialize genesis account - use the hash from your loadgen output
	genesisAccount := "842b4e0a5f891d7ddd612bbed4e006b30029777618089e11e4434b23aa8f436e"
	genesisKey := fmt.Sprintf("player:%s:balance", genesisAccount)
	l.State.Set([]byte(genesisKey), []byte("1000000000000000000000"))
	log.Printf("Leader: initialized genesis account %s with 10000M balance", genesisAccount)
	
	for x := 0; x < size; x++ {
		for y := 0; y < size; y++ {
			plotID := fmt.Sprintf("plot-%d-%d", x, y)
			ownerKey := fmt.Sprintf("land:%s:owner", plotID)
			if b, ok := l.State.Get([]byte(ownerKey)); !ok || len(b) == 0 {
				l.State.Set([]byte(ownerKey), []byte(""))
			}
			quotaKey := fmt.Sprintf("land:%s:quota", plotID)
			if b, ok := l.State.Get([]byte(quotaKey)); !ok || len(b) == 0 {
				l.State.Set([]byte(quotaKey), []byte("0"))
			}
			resKey := fmt.Sprintf("land:%s:resource", plotID)
			if b, ok := l.State.Get([]byte(resKey)); !ok || len(b) == 0 {
				l.State.Set([]byte(resKey), []byte("0"))
			}
		}
	}
	log.Printf("Leader %s: initialized world grid %dx%d", l.ID, size, size)
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
			// signature check delegated to workers; leader accepts valid formatted tx
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

			// include authoritative snapshot
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

// === Block ticker and month enforcement ===
// BlockTicker advances height, commits blocks, and runs month-end enforcement when appropriate.
func (l *Leader) BlockTicker(ctx context.Context, interval time.Duration, monthBlockCount int) {
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

	log.Printf("Leader %s: starting block ticker (interval=%s monthBlocks=%d)", l.ID, interval.String(), monthBlockCount)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// advance height
			l.blockMu.Lock()
			l.height++
			height := l.height
			l.blockMu.Unlock()

			// grab pending
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

			// commit block if there are txs
			if len(collected) > 0 {
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
				} else {
					// append to index
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

			// month-end check
			if monthBlockCount > 0 && (height%monthBlockCount) == 0 {
				monthNum := height / monthBlockCount
				log.Printf("Leader %s: month #%d boundary reached (height=%d) — running tax enforcement", l.ID, monthNum, height)
				if err := l.runMonthEndEnforcement(monthNum); err != nil {
					log.Printf("Leader %s: month enforcement error: %v", l.ID, err)
				}
			}
		}
	}
}

// runMonthEndEnforcement iterates over land entries and enforces tax rules.
// Writes month-summary-<n>.json into blocks/.
func (l *Leader) runMonthEndEnforcement(month int) error {
	// Configurable rules (tweak these)
	const taxRate = 1.0 / 10.0   // 10%
	const penaltyRate = 1.0 / 5.0 // 20%

	// snapshot copy
	snap := l.State.GetSnapshot()

	type summaryEntry struct {
		PlotID     string `json:"plot_id"`
		Owner      string `json:"owner"`
		Resource   int    `json:"resource"`
		TaxDue     int    `json:"tax_due"`
		TaxPaid    bool   `json:"tax_paid"`
		Penalty    int    `json:"penalty_applied"`
		OwnerAfter int    `json:"owner_balance_after"`
		Note       string `json:"note,omitempty"`
	}

	summary := make([]summaryEntry, 0)

	// helpers
	readIntFromSnap := func(key string) int {
		if v, ok := snap[key]; ok && len(v) > 0 {
			if n, err := strconv.Atoi(string(v)); err == nil {
				return n
			}
		}
		return 0
	}

	applySet := func(k string, v []byte) {
		l.State.Set([]byte(k), v)
	}

	// collect plot IDs by scanning snapshot keys
	seenPlots := map[string]struct{}{}
	for k := range snap {
		if strings.HasPrefix(k, "land:") {
			parts := strings.Split(k, ":")
			if len(parts) >= 3 {
				plotID := parts[1]
				seenPlots[plotID] = struct{}{}
			}
		}
	}

	for plotID := range seenPlots {
		ownerKey := fmt.Sprintf("land:%s:owner", plotID)
		owner := ""
		if b, ok := l.State.Get([]byte(ownerKey)); ok && len(b) > 0 {
			owner = string(b)
		}
		resource := readIntFromSnap(fmt.Sprintf("land:%s:resource", plotID))
		quota := readIntFromSnap(fmt.Sprintf("land:%s:quota", plotID))

		var taxDue int
		if quota > 0 {
			taxDue = quota
		} else {
			taxDue = int(float64(resource) * taxRate)
		}
		if taxDue < 0 {
			taxDue = 0
		}

		entry := summaryEntry{
			PlotID:   plotID,
			Owner:    owner,
			Resource: resource,
			TaxDue:   taxDue,
		}

		if owner == "" {
			entry.Note = "unowned plot"
			summary = append(summary, entry)
			continue
		}

		ownerBalKey := fmt.Sprintf("player:%s:balance", owner)
		ownerBal := 0
		if b, ok := l.State.Get([]byte(ownerBalKey)); ok && len(b) > 0 {
			if n, err := strconv.Atoi(string(b)); err == nil {
				ownerBal = n
			}
		}

		if ownerBal >= taxDue {
			ownerBalAfter := ownerBal - taxDue
			applySet(ownerBalKey, []byte(strconv.Itoa(ownerBalAfter)))
			applySet(fmt.Sprintf("land:%s:tax_paid", plotID), []byte("true"))
			entry.TaxPaid = true
			entry.OwnerAfter = ownerBalAfter
		} else {
			penalty := int(float64(resource) * penaltyRate)
			if penalty < 0 {
				penalty = 0
			}
			newResource := resource - penalty
			if newResource < 0 {
				newResource = 0
			}
			applySet(fmt.Sprintf("land:%s:resource", plotID), []byte(strconv.Itoa(newResource)))
			applySet(fmt.Sprintf("land:%s:tax_paid", plotID), []byte("false"))
			entry.TaxPaid = false
			entry.Penalty = penalty
			entry.OwnerAfter = ownerBal
			entry.Note = "insufficient balance; penalty applied"
		}

		summary = append(summary, entry)
	}

	// write summary
		if len(summary) > 0 {
			blocksDir := "blocks"
			_ = os.MkdirAll(blocksDir, 0755)
			filename := fmt.Sprintf("month-summary-%06d.json", month)
			fullPath := filepath.Join(blocksDir, filename)
			if data, err := json.MarshalIndent(summary, "", "  "); err == nil {
				_ = os.WriteFile(fullPath, data, 0644)
			} else {
				log.Printf("Leader %s: failed to marshal month summary: %v", l.ID, err)
			}
			log.Printf("Leader %s: month %d enforcement complete — plots=%d", l.ID, month, len(summary))
		} else {
			log.Printf("Leader %s: month %d enforcement skipped (no activity)", l.ID, month)
		}
		return nil
}
