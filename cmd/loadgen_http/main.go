package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Actions for spam (no AllocateLand, seeding handles that)
var actions = []string{"Grow", "Transfer", "PayTax"}

// Tracks plots owned per player
var playerPlots = make(map[string][]string)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// Transaction struct matching the API
type Transaction struct {
	ID      string            `json:"ID"`
	Sender  string            `json:"Sender"`
	Nonce   uint64            `json:"Nonce"`
	Action  string            `json:"Action"`
	Payload map[string]string `json:"Payload"`
	PubKey  string            `json:"PubKey"`
	Sig     string            `json:"Sig"`
}

// canonicalize produces the same JSON format as JavaScript frontend
func canonicalize(sender string, nonce uint64, action string, payload map[string]string) string {
	// Sort payload keys
	keys := make([]string, 0, len(payload))
	for k := range payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build JSON with alphabetical field order: action, nonce, payload, sender
	var parts []string

	actionJSON, _ := json.Marshal(action)
	parts = append(parts, `"action":`+string(actionJSON))

	parts = append(parts, fmt.Sprintf(`"nonce":%d`, nonce))

	if len(payload) > 0 {
		var payloadParts []string
		for _, k := range keys {
			keyJSON, _ := json.Marshal(k)
			valJSON, _ := json.Marshal(payload[k])
			payloadParts = append(payloadParts, string(keyJSON)+":"+string(valJSON))
		}
		parts = append(parts, `"payload":{`+strings.Join(payloadParts, ",")+`}`)
	} else {
		parts = append(parts, `"payload":{}`)
	}

	senderJSON, _ := json.Marshal(sender)
	parts = append(parts, `"sender":`+string(senderJSON))

	return "{" + strings.Join(parts, ",") + "}"
}

// sendTxn builds, signs with ECDSA P-256, and POSTs a transaction
func sendTxn(ctx context.Context, priv *ecdsa.PrivateKey, sender, action string, payload map[string]string, api string) string {
	nonce := uint64(time.Now().UnixNano())

	// Canonicalize for signing
	canonical := canonicalize(sender, nonce, action, payload)
	hash := sha256.Sum256([]byte(canonical))

	// Sign with ECDSA
	r, s, err := ecdsa.Sign(rand.Reader, priv, hash[:])
	if err != nil {
		log.Printf("sign error: %v", err)
		return ""
	}

	// Convert signature to P1363 format (r||s, 32 bytes each for P-256)
	sigBytes := make([]byte, 64)
	r.FillBytes(sigBytes[0:32])
	s.FillBytes(sigBytes[32:64])

	// Export public key as SPKI
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		log.Printf("marshal pubkey error: %v", err)
		return ""
	}

	tx := Transaction{
		ID:      "",
		Sender:  sender,
		Nonce:   nonce,
		Action:  action,
		Payload: payload,
		PubKey:  base64.StdEncoding.EncodeToString(pubBytes),
		Sig:     base64.StdEncoding.EncodeToString(sigBytes),
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
		body := make([]byte, 1024)
		resp.Body.Read(body)
		log.Printf("http error status=%d body=%s", resp.StatusCode, string(body))
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

	// Generate ECDSA P-256 key
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}

	// Compute sender ID from public key (SHA256 of SPKI)
	pubBytes, _ := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	senderHash := sha256.Sum256(pubBytes)
	baseSender := fmt.Sprintf("%x", senderHash[:])

	log.Printf("Base sender ID: %s", baseSender)

	// ---------------------------------------------------
	// SEEDING PHASE
	// ---------------------------------------------------
	log.Println("Seeding state (faucet -> players; AllocateLand per player)...")

	// 1) Faucet -> players (use base sender as player0)
	for w := 1; w < *workers; w++ {  // Start from 1, skip player0
    player := fmt.Sprintf("player%d", w)
    payload := map[string]string{
        "from":   baseSender,  // Changed from "faucet"
        "to":     player,
        "amount": "10000",
    }
    sendTxn(ctx, priv, baseSender, "Transfer", payload, *api)
}

	// 2) Allocate plots (only for player 0 = our key)
	for i := 0; i < 500000; i++ {
		plotID := fmt.Sprintf("plot-0-%d", i)
		playerPlots[baseSender] = append(playerPlots[baseSender], plotID)
		payload := map[string]string{
			"plotID": plotID,
			"owner":  baseSender,
		}
		sendTxn(ctx, priv, baseSender, "AllocateLand", payload, *api)
	}

	log.Printf("Seeding done — waiting %s before spam", seedWait.String())
	time.Sleep(*seedWait)

	// ---------------------------------------------------
	// SPAM PHASE (only player0 = our key can send)
	// ---------------------------------------------------
// ---------------------------------------------------
// SPAM PHASE
// ---------------------------------------------------
log.Println("Starting spam workers now...")

var wg sync.WaitGroup
for w := 0; w < *workers; w++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()
        ticker := time.NewTicker(time.Second / time.Duration(*rate))
        defer ticker.Stop()

        for range ticker.C {
            if total > 0 && atomic.LoadInt64(&sent) >= total {
                return
            }
            
            // Only use actions that work with single account
            validActions := []string{"Grow", "PayTax"}
            action := validActions[mrand.Intn(len(validActions))]
            
            var payload map[string]string
            switch action {
            case "Grow":
                plots := playerPlots[baseSender]
                if len(plots) == 0 {
                    continue
                }
                plotID := plots[mrand.Intn(len(plots))]
                payload = map[string]string{
                    "plotID": plotID,
                    "amount": fmt.Sprintf("%d", mrand.Intn(10)+1),
                }
            case "PayTax":
                plots := playerPlots[baseSender]
                if len(plots) == 0 {
                    continue
                }
                plotID := plots[mrand.Intn(len(plots))]
                payload = map[string]string{
                    "plotID": plotID,
                    "amount": fmt.Sprintf("%d", mrand.Intn(5)+1),
                }
            }
            
            if payload != nil {
                sendTxn(ctx, priv, baseSender, action, payload, *api)
                atomic.AddInt64(&sent, 1)
            }
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