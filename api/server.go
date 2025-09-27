package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
		h.Write(tx.PubKey)
	}
	if len(tx.Sig) > 0 {
		h.Write(tx.Sig)
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
	if err != nil || len(entries) == 0 {
		http.Error(w, "no month summaries available", http.StatusNotFound)
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
		http.Error(w, "no month summaries found", http.StatusNotFound)
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
					"payload": tx.Payload, // will be real JSON, not [object Object]
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
