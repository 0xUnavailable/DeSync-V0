package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MelzLabs/DeSync/network"
	"github.com/MelzLabs/DeSync/state"
	"github.com/MelzLabs/DeSync/types"
)

// Server exposes read + write endpoints for the DeSync node.
type Server struct {
	Node      *network.Node
	Store     *state.MemStore
	BlockDir  string
	Addr      string
	StaticDir string
}

func NewServer(node *network.Node, store *state.MemStore, blockDir, addr, staticDir string) *Server {
	return &Server{
		Node:      node,
		Store:     store,
		BlockDir:  blockDir,
		Addr:      addr,
		StaticDir: staticDir,
	}
}

// Start starts the HTTP server (blocking).
func (s *Server) Start() {
	// API endpoints
	http.HandleFunc("/status", s.handleStatus)
	http.HandleFunc("/state/land/", s.handleLand)
	http.HandleFunc("/state/player/", s.handlePlayer)
	http.HandleFunc("/state/world", s.handleWorld) // ✅ new endpoint
	http.HandleFunc("/lastRoot", s.handleLastRoot)
	http.HandleFunc("/blocks/", s.handleBlock)
	http.HandleFunc("/blocks/latest", s.handleLast)
	http.HandleFunc("/months/", s.handleMonthSummary)
	http.HandleFunc("/months/latest", s.handleLatestMonth)
	http.HandleFunc("/tx", s.handleTx)
	http.HandleFunc("/txs/all", s.handleAllTxs)
	http.HandleFunc("/txs/block/", s.handleBlockTxs)

	// Serve static files (explorer UI)
	if s.StaticDir != "" {
		fs := http.FileServer(http.Dir(s.StaticDir))
		http.Handle("/", fs)
	}

	srv := &http.Server{
		Addr:         s.Addr,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 20 * time.Second,
	}

	log.Printf("API server listening on %s", s.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("API server failed: %v", err)
	}
}

// ---------------- Helpers ----------------

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func handlePreflight(w http.ResponseWriter, r *http.Request) bool {
	enableCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func serveFileOr404(w http.ResponseWriter, path string) {
	enableCORS(w)
	data, err := os.ReadFile(path)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func bytesToStr(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	enableCORS(w)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func computeTxID(tx *types.Transaction) string {
	h := sha256.New()
	h.Write([]byte(tx.Sender))
	h.Write([]byte(fmt.Sprintf("%d", tx.Nonce)))
	h.Write([]byte(tx.Action))
	if tx.Payload != nil {
		keys := make([]string, 0, len(tx.Payload))
		for k := range tx.Payload {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.Write([]byte(k))
			h.Write([]byte(tx.Payload[k]))
		}
	}
	if len(tx.PubKey) > 0 {
		h.Write([]byte(tx.PubKey))  // Convert string to []byte
	}
	if len(tx.Sig) > 0 {
		h.Write([]byte(tx.Sig))  // Convert string to []byte
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ---------------- Handlers ----------------

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}

	var peerID string
	var nodeRole string
	var isReady bool

	if s.Node != nil && s.Node.Host != nil {
		peerID = s.Node.Host.ID().String()
		isReady = true
		if s.Node.TxTopic != nil {
			nodeRole = "leader"
		} else {
			nodeRole = "worker"
		}
	} else {
		peerID = "unknown"
		nodeRole = "unknown"
		isReady = false
	}

	stateSize := 0
	if s.Store != nil {
		snap := s.Store.GetSnapshot()
		stateSize = len(snap)
	}

	blockCount := 0
	if s.BlockDir != "" {
		if entries, err := os.ReadDir(s.BlockDir); err == nil {
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), "block-") && strings.HasSuffix(e.Name(), ".json") {
					blockCount++
				}
			}
		}
	}

	resp := map[string]interface{}{
		"peer_id":     peerID,
		"role":        nodeRole,
		"ready":       isReady,
		"state_keys":  stateSize,
		"block_count": blockCount,
		"timestamp":   time.Now().Unix(),
		"api_version": "1.0",
	}

	writeJSON(w, resp)
}

// canonicalTxBytes produces deterministic bytes for signing/verifying.
// It mirrors the JS canonicalize for fields: sender, nonce, action, payload (map[string]string)
func canonicalTxBytes(sender string, nonce uint64, action string, payload map[string]string) ([]byte, error) {
    // Match JavaScript canonicalize exactly
    // Order: action, nonce, payload, sender (alphabetical)
    
    var parts []string
    
    // action
    actionJSON, _ := json.Marshal(action)
    parts = append(parts, `"action":`+string(actionJSON))
    
    // nonce
    parts = append(parts, fmt.Sprintf(`"nonce":%d`, nonce))
    
    // payload with sorted keys
    if len(payload) > 0 {
        keys := make([]string, 0, len(payload))
        for k := range payload {
            keys = append(keys, k)
        }
        sort.Strings(keys)
        
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
    
    // sender
    senderJSON, _ := json.Marshal(sender)
    parts = append(parts, `"sender":`+string(senderJSON))
    
    canonical := "{" + strings.Join(parts, ",") + "}"
    return []byte(canonical), nil
}
// verifyTxSignature verifies base64 SPKI public key and base64 signature against canonical tx bytes.
// Returns nil on success.
func verifyTxSignature(txSender string, nonce uint64, action string, payload map[string]string, pubKeyB64 string, sigB64 string) error {
    if pubKeyB64 == "" || sigB64 == "" {
        return errors.New("missing pubkey or signature")
    }
    
    // decode pubkey (SPKI, DER)
    pubBytes, err := base64.StdEncoding.DecodeString(pubKeyB64)
    if err != nil {
        return fmt.Errorf("pubkey base64 decode: %w", err)
    }
    
    // parse spki
    keyIface, err := x509.ParsePKIXPublicKey(pubBytes)
    if err != nil {
        return fmt.Errorf("parse pubkey spki failed: %w", err)
    }
    ecdsaPub, ok := keyIface.(*ecdsa.PublicKey)
    if !ok {
        return fmt.Errorf("pubkey not ecdsa")
    }
    
    // Build canonical message bytes
    msg, err := canonicalTxBytes(txSender, nonce, action, payload)
    if err != nil {
        return fmt.Errorf("canonicalize failed: %w", err)
    }
    
    h := sha256.Sum256(msg)
    
    // decode signature
    sigBytes, err := base64.StdEncoding.DecodeString(sigB64)
    if err != nil {
        return fmt.Errorf("sig base64 decode: %w", err)
    }
    
    // verify - JavaScript produces IEEE P1363 format (r||s, 64 bytes for P-256)
    if len(sigBytes) != 64 {
        return fmt.Errorf("expected 64-byte P1363 signature, got %d bytes", len(sigBytes))
    }

    // Split into r and s (32 bytes each for P-256)
    r := new(big.Int).SetBytes(sigBytes[:32])
    s := new(big.Int).SetBytes(sigBytes[32:])

    if !ecdsa.Verify(ecdsaPub, h[:], r, s) {
        return fmt.Errorf("signature verification failed")
    }
    
    // verify that txSender matches hash of pubkey (identity binding)
    pubHash := sha256.Sum256(pubBytes)
    expectedSender := hex.EncodeToString(pubHash[:])
    if txSender != expectedSender {
        return fmt.Errorf("sender mismatch: tx.sender=%s computed=%s", txSender, expectedSender)
    }
    
    return nil
}

func (s *Server) handleLand(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/state/land/")
	if id == "" {
		http.Error(w, "missing land id", http.StatusBadRequest)
		return
	}
	ownerB, _ := s.Store.Get([]byte(fmt.Sprintf("land:%s:owner", id)))
	resourceB, _ := s.Store.Get([]byte(fmt.Sprintf("land:%s:resource", id)))
	quotaB, _ := s.Store.Get([]byte(fmt.Sprintf("land:%s:quota", id)))
	taxB, _ := s.Store.Get([]byte(fmt.Sprintf("land:%s:tax_paid", id)))

	resp := map[string]string{
		"id":       id,
		"owner":    bytesToStr(ownerB),
		"resource": bytesToStr(resourceB),
		"quota":    bytesToStr(quotaB),
		"tax_paid": bytesToStr(taxB),
	}
	writeJSON(w, resp)
}

func (s *Server) handlePlayer(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/state/player/")
	if id == "" {
		http.Error(w, "missing player id", http.StatusBadRequest)
		return
	}
	balB, _ := s.Store.Get([]byte(fmt.Sprintf("player:%s:balance", id)))
	resp := map[string]string{
		"id":      id,
		"balance": bytesToStr(balB),
	}
	writeJSON(w, resp)
}

// ✅ New handler for full world snapshot
func (s *Server) handleWorld(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}

	snap := s.Store.GetSnapshot()

	type plot struct {
		ID       string `json:"id"`
		Owner    string `json:"owner"`
		Resource int    `json:"resource"`
		Quota    int    `json:"quota"`
	}

	seen := map[string]struct{}{}
	for k := range snap {
		if strings.HasPrefix(k, "land:") {
			parts := strings.Split(k, ":")
			if len(parts) >= 3 {
				seen[parts[1]] = struct{}{}
			}
		}
	}

	plots := make([]plot, 0, len(seen))
	for pid := range seen {
		owner := ""
		if b, ok := s.Store.Get([]byte(fmt.Sprintf("land:%s:owner", pid))); ok && len(b) > 0 {
			owner = string(b)
		}
		resource := 0
		if b, ok := snap[fmt.Sprintf("land:%s:resource", pid)]; ok && len(b) > 0 {
			if n, err := strconv.Atoi(string(b)); err == nil {
				resource = n
			}
		}
		quota := 0
		if b, ok := snap[fmt.Sprintf("land:%s:quota", pid)]; ok && len(b) > 0 {
			if n, err := strconv.Atoi(string(b)); err == nil {
				quota = n
			}
		}
		plots = append(plots, plot{
			ID:       pid,
			Owner:    owner,
			Resource: resource,
			Quota:    quota,
		})
	}

	writeJSON(w, plots)
}

func (s *Server) handleLastRoot(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	snap := s.Store.GetSnapshot()
	resp := map[string]interface{}{
		"keys": len(snap),
		"root": "dummy-root",
	}
	writeJSON(w, resp)
}

func (s *Server) handleBlock(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	trim := strings.TrimPrefix(r.URL.Path, "/blocks/")
	if trim == "" {
		http.Error(w, "missing block id", http.StatusBadRequest)
		return
	}
	if trim == "latest" {
		s.handleLast(w, r)
		return
	}
	n, err := strconv.Atoi(trim)
	if err == nil {
		filename := fmt.Sprintf("block-%06d.json", n)
		path := filepath.Join(s.BlockDir, filename)
		serveFileOr404(w, path)
		return
	}
	path := filepath.Join(s.BlockDir, fmt.Sprintf("block-%s.json", trim))
	serveFileOr404(w, path)
}

func (s *Server) handleLast(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	entries, err := os.ReadDir(s.BlockDir)
	if err != nil || len(entries) == 0 {
		http.Error(w, "no blocks available", http.StatusNotFound)
		return
	}
	max := -1
	var latestName string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "block-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		numStr := strings.TrimSuffix(strings.TrimPrefix(name, "block-"), ".json")
		n, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		if n > max {
			max = n
			latestName = name
		}
	}
	if latestName == "" {
		http.Error(w, "no blocks found", http.StatusNotFound)
		return
	}
	path := filepath.Join(s.BlockDir, latestName)
	serveFileOr404(w, path)
}

func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var tx types.Transaction
	if err := json.Unmarshal(body, &tx); err != nil {
		http.Error(w, "invalid transaction JSON", http.StatusBadRequest)
		return
	}

	// 🔐 Verify signature if PubKey & Sig are present
	if len(tx.PubKey) == 0 || len(tx.Sig) == 0 {
		http.Error(w, "missing pubkey or signature", http.StatusBadRequest)
		return
	}

	if err := verifyTxSignature(tx.Sender, tx.Nonce, tx.Action, tx.Payload, tx.PubKey, tx.Sig); err != nil {
		http.Error(w, "invalid signature: "+err.Error(), http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(tx.ID) == "" {
		tx.ID = computeTxID(&tx)
	}

	msg := network.TxMessage{Tx: &tx}
	data, err := network.EncodeTx(msg)
	if err != nil {
		http.Error(w, "failed to encode tx", http.StatusInternalServerError)
		return
	}

	if s.Node == nil || s.Node.TxTopic == nil {
		http.Error(w, "node tx topic not available on server", http.StatusInternalServerError)
		return
	}

	if err := s.Node.TxTopic.Publish(context.Background(), data); err != nil {
		http.Error(w, fmt.Sprintf("failed to publish tx: %v", err), http.StatusInternalServerError)
		return
	}

	resp := map[string]string{"status": "submitted", "txid": tx.ID}
	writeJSON(w, resp)
}


func (s *Server) handleMonthSummary(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	nStr := strings.TrimPrefix(r.URL.Path, "/months/")
	if nStr == "" {
		http.Error(w, "missing month id", http.StatusBadRequest)
		return
	}
	if nStr == "latest" {
		s.handleLatestMonth(w, r)
		return
	}
	n, err := strconv.Atoi(nStr)
	if err != nil {
		http.Error(w, "invalid month id", http.StatusBadRequest)
		return
	}
	filename := fmt.Sprintf("month-summary-%06d.json", n)
	path := filepath.Join(s.BlockDir, filename)
	serveFileOr404(w, path)
}

func (s *Server) handleLatestMonth(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	entries, err := os.ReadDir(s.BlockDir)
	if err != nil {
		http.Error(w, "failed to read blocks dir", http.StatusInternalServerError)
		return
	}

	max := -1
	var latestName string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "month-summary-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		numStr := strings.TrimSuffix(strings.TrimPrefix(name, "month-summary-"), ".json")
		n, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		if n > max {
			max = n
			latestName = name
		}
	}

	if latestName == "" {
		// no month summaries written yet → respond with empty JSON list
		writeJSON(w, []interface{}{})
		return
	}

	path := filepath.Join(s.BlockDir, latestName)
	serveFileOr404(w, path)
}

// ---------------- Transaction History ----------------

func (s *Server) handleAllTxs(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	entries, err := os.ReadDir(s.BlockDir)
	if err != nil {
		http.Error(w, "failed to read blocks dir", http.StatusInternalServerError)
		return
	}

	var allTxs []map[string]interface{}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "block-") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.BlockDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var block struct {
			Txs []types.Transaction `json:"txs"`
		}
		if err := json.Unmarshal(data, &block); err == nil {
			for _, tx := range block.Txs {
				txMap := map[string]interface{}{
					"id":      tx.ID,
					"sender":  tx.Sender,
					"nonce":   tx.Nonce,
					"action":  tx.Action,
					"payload": tx.Payload,
				}
				allTxs = append(allTxs, txMap)
			}
		}
	}

	writeJSON(w, allTxs)
}

func (s *Server) handleBlockTxs(w http.ResponseWriter, r *http.Request) {
	if handlePreflight(w, r) {
		return
	}
	blockID := strings.TrimPrefix(r.URL.Path, "/txs/block/")
	if blockID == "" {
		http.Error(w, "missing block id", http.StatusBadRequest)
		return
	}

	filename := fmt.Sprintf("block-%06s.json", blockID)
	path := filepath.Join(s.BlockDir, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		http.Error(w, "block not found", http.StatusNotFound)
		return
	}

	var block struct {
		Txs []types.Transaction `json:"txs"`
	}
	if err := json.Unmarshal(data, &block); err != nil {
		http.Error(w, "invalid block json", http.StatusInternalServerError)
		return
	}

	var txs []map[string]interface{}
	for _, tx := range block.Txs {
		txMap := map[string]interface{}{
			"id":      tx.ID,
			"sender":  tx.Sender,
			"nonce":   tx.Nonce,
			"action":  tx.Action,
			"payload": tx.Payload,
		}
		txs = append(txs, txMap)
	}

	writeJSON(w, txs)
}
