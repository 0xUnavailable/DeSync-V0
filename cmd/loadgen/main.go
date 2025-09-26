package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MelzLabs/DeSync/network"
	"github.com/MelzLabs/DeSync/types"
	"github.com/libp2p/go-libp2p/core/crypto"
)

// Actions for spam (don't include AllocateLand here; seeding handles that)
var actions = []string{"Grow", "Transfer", "PayTax"}

// Tracks plots owned per player
var playerPlots = make(map[string][]string)

// sendTxn creates, signs, publishes a tx and returns its Tx.ID
func sendTxn(ctx context.Context, node *network.Node, priv ed25519.PrivateKey,
	sender, action string, payload map[string]string) string {

	tx := types.NewTxn(sender, action, payload)
	tx.Nonce = uint64(time.Now().UnixNano())
	if err := tx.Sign(priv); err != nil {
		// sign failed — return empty id
		return ""
	}
	msg := network.TxMessage{Tx: tx}
	data, _ := network.EncodeTx(msg)
	_ = node.TxTopic.Publish(ctx, data)
	return tx.ID
}

// randomPayload chooses valid payloads constrained to seeded state (sender-owned plots)
func randomPayload(action string, sender string, workers int) map[string]string {
	args := map[string]string{}
	switch action {
	case "Grow":
		plots := playerPlots[sender]
		if len(plots) == 0 {
			return nil
		}
		plotID := plots[mrand.Intn(len(plots))]
		args["plotID"] = plotID
		args["amount"] = fmt.Sprintf("%d", mrand.Intn(10)+1)
	case "Transfer":
		// transfer small amounts, to another player
		to := sender
		for to == sender {
			to = fmt.Sprintf("player%d", mrand.Intn(workers))
		}
		args["from"] = sender
		args["to"] = to
		args["amount"] = fmt.Sprintf("%d", mrand.Intn(25)+1)
	case "PayTax":
		plots := playerPlots[sender]
		if len(plots) == 0 {
			return nil
		}
		plotID := plots[mrand.Intn(len(plots))]
		args["plotID"] = plotID
		args["amount"] = fmt.Sprintf("%d", mrand.Intn(10)+1)
	}
	return args
}

func main() {
	connect := flag.String("connect", "", "multiaddr to bootstrap connect (any node)")
	workers := flag.Int("workers", 5, "number of tx senders")
	rate := flag.Int("rate", 100, "tx per second per worker")
	txCount := flag.Int("txcount", 0, "total number of transactions to send (0 = unlimited)")
	seedTimeout := flag.Duration("seedtimeout", 30*time.Second, "how long to wait for seeding confirmations")
	flag.Parse()

	if *connect == "" {
		log.Fatal("need --connect")
	}

	ctx := context.Background()

	// libp2p node identity
	lp2pPriv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	node, err := network.NewNodeWithKey(ctx, "/ip4/0.0.0.0/tcp/0", lp2pPriv)
	if err != nil {
		log.Fatal(err)
	}
	if err := node.Connect(ctx, *connect); err != nil {
		log.Fatal(err)
	}

	// tx signing key
	_, txPriv, _ := ed25519.GenerateKey(rand.Reader)

	mrand.Seed(time.Now().UnixNano())
	log.Printf("Starting loadgen with %d workers at %d tx/s each (target=%d)", *workers, *rate, *txCount)

	total := int64(*txCount)
	var sent int64

	// subscribe to results (for seeding confirmation)
	sub, err := node.ResultTopic.Subscribe()
	if err != nil {
		log.Fatalf("failed to subscribe to result topic: %v", err)
	}

	// ---------------------------------------------------
	// SEEDING PHASE
	// ---------------------------------------------------
	log.Println("Seeding state (faucet -> players; AllocateLand per player)...")

	faucet := "faucet"
	seedTxs := make(map[string]struct{})
	var seedMu sync.Mutex
	confirmed := int32(0)

	// start listener for confirmations (marks seed txs as confirmed)
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
			// iterate results and mark matching seed txs
			for _, r := range sr.Results {
				if r == nil {
					continue
				}
				seedMu.Lock()
				if _, ok := seedTxs[r.TxID]; ok && r.Success {
					delete(seedTxs, r.TxID)
					atomic.AddInt32(&confirmed, 1)
				}
				seedMu.Unlock()
			}
		}
	}()

	// 1) Faucet -> players (funding)
	for w := 0; w < *workers; w++ {
		player := fmt.Sprintf("player%d", w)
		payload := map[string]string{
			"from":   faucet,
			"to":     player,
			"amount": "10000",
		}
		txid := sendTxn(ctx, node, txPriv, faucet, "Transfer", payload)
		if txid != "" {
			seedMu.Lock()
			seedTxs[txid] = struct{}{}
			seedMu.Unlock()
		}
	}

	// 2) Allocate plots per player and record them locally
	for w := 0; w < *workers; w++ {
		player := fmt.Sprintf("player%d", w)
		for i := 0; i < 5; i++ {
			plotID := fmt.Sprintf("plot-%d-%d", w, i)
			playerPlots[player] = append(playerPlots[player], plotID)
			payload := map[string]string{
				"plotID": plotID,
				"owner":  player,
			}
			txid := sendTxn(ctx, node, txPriv, player, "AllocateLand", payload)
			if txid != "" {
				seedMu.Lock()
				seedTxs[txid] = struct{}{}
				seedMu.Unlock()
			}
		}
	}

	// wait until all seeds are confirmed or until timeout
	start := time.Now()
	expectedSeeds := 0
	seedMu.Lock()
	expectedSeeds = len(seedTxs)
	seedMu.Unlock()

	log.Printf("Waiting up to %s for %d seed confirmations...", seedTimeout.String(), expectedSeeds)
	for {
		seedMu.Lock()
		remaining := len(seedTxs)
		seedMu.Unlock()
		if remaining == 0 {
			log.Printf("All %d seed txs confirmed.", expectedSeeds)
			break
		}
		if time.Since(start) > *seedTimeout {
			log.Printf("Seed timeout reached: %d seed txs still unconfirmed (continuing)...", remaining)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// ---------------------------------------------------
	// START SPAM WORKERS (only after seeding wait)
	// ---------------------------------------------------
	log.Println("Starting spam workers now...")

	for w := 0; w < *workers; w++ {
		go func(id int) {
			sender := fmt.Sprintf("player%d", id)
			ticker := time.NewTicker(time.Second / time.Duration(*rate))
			defer ticker.Stop()

			for range ticker.C {
				if total > 0 && atomic.LoadInt64(&sent) >= total {
					return
				}
				action := actions[mrand.Intn(len(actions))]
				payload := randomPayload(action, sender, *workers)
				if payload == nil {
					continue
				}
				sendTxn(ctx, node, txPriv, sender, action, payload)
				atomic.AddInt64(&sent, 1)
			}
		}(w)
	}

	// exit/wait logic
	if total > 0 {
		for {
			if atomic.LoadInt64(&sent) >= total {
				log.Printf("Loadgen finished: sent %d transactions", total)
				return
			}
			time.Sleep(1 * time.Second)
		}
	} else {
		select {}
	}
}
