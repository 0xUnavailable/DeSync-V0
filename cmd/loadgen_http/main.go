package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MelzLabs/DeSync/types"
)

// Actions for spam (no AllocateLand, seeding handles that)
var actions = []string{"Grow", "Transfer", "PayTax"}

// Tracks plots owned per player
var playerPlots = make(map[string][]string)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// sendTxn builds, signs, and POSTs a transaction
func sendTxn(ctx context.Context, priv ed25519.PrivateKey, sender, action string, payload map[string]string, api string) string {
	tx := types.NewTxn(sender, action, payload)
	tx.Nonce = uint64(time.Now().UnixNano())
	if err := tx.Sign(priv); err != nil {
		return ""
	}

	b, _ := json.Marshal(tx)
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(api, "/")+"/tx", strings.NewReader(string(b)))
	if err != nil {
		log.Printf("request error: %v", err)
		return ""
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("http post error: %v", err)
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("http error status=%d", resp.StatusCode)
		return ""
	}
	return tx.ID
}

// randomPayload chooses valid payloads constrained to seeded state
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
	api := flag.String("api", "http://127.0.0.1:8080", "HTTP API base URL")
	workers := flag.Int("workers", 5, "number of tx senders")
	rate := flag.Int("rate", 100, "tx per second per worker")
	txCount := flag.Int("txcount", 0, "total number of transactions to send (0 = unlimited)")
	seedWait := flag.Duration("seedwait", 5*time.Second, "how long to wait after seeding before spam")
	flag.Parse()

	if *api == "" {
		log.Fatal("need --api (e.g. http://127.0.0.1:8080)")
	}

	ctx := context.Background()
	mrand.Seed(time.Now().UnixNano())

	log.Printf("HTTP loadgen starting: api=%s workers=%d rate=%d txcount=%d", *api, *workers, *rate, *txCount)

	total := int64(*txCount)
	var sent int64

	// One signing key reused for all players
	_, priv, _ := ed25519.GenerateKey(rand.Reader)

	// ---------------------------------------------------
	// SEEDING PHASE
	// ---------------------------------------------------
	log.Println("Seeding state (faucet -> players; AllocateLand per player)...")

	faucet := "faucet"

	// 1) Faucet -> players
	for w := 0; w < *workers; w++ {
		player := fmt.Sprintf("player%d", w)
		payload := map[string]string{
			"from":   faucet,
			"to":     player,
			"amount": "10000",
		}
		sendTxn(ctx, priv, faucet, "Transfer", payload, *api)
	}

	// 2) Allocate plots
	for w := 0; w < *workers; w++ {
		player := fmt.Sprintf("player%d", w)
		for i := 0; i < 5; i++ {
			plotID := fmt.Sprintf("plot-%d-%d", w, i)
			playerPlots[player] = append(playerPlots[player], plotID)
			payload := map[string]string{
				"plotID": plotID,
				"owner":  player,
			}
			sendTxn(ctx, priv, player, "AllocateLand", payload, *api)
		}
	}

	log.Printf("Seeding done — waiting %s before spam", seedWait.String())
	time.Sleep(*seedWait)

	// ---------------------------------------------------
	// SPAM PHASE
	// ---------------------------------------------------
	log.Println("Starting spam workers now...")

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
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
				sendTxn(ctx, priv, sender, action, payload, *api)
				atomic.AddInt64(&sent, 1)
			}
		}(w)
	}

	if total > 0 {
		for {
			if atomic.LoadInt64(&sent) >= total {
				log.Printf("Loadgen finished: sent %d transactions", total)
				break
			}
			time.Sleep(1 * time.Second)
		}
	} else {
		wg.Wait()
	}
}
