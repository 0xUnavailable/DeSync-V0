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
// It publishes transactions to the node's TxTopic and reads state from the provided store.
type Server struct {
	Node     *network.Node
	Store    *state.MemStore
	BlockDir string
	Addr     string
}

// NewServer creates a new API server.
// node — network node (used to publish txs to TxTopic).
// store — authoritative memstore used by leader.
// blockDir — directory where leader writes block-<height>.json files.
// addr — listen address (e.g. ":8080").
func NewServer(node *network.Node, store *state.MemStore, blockDir, addr string) *Server {
	return &Server{
		Node:     node,
		Store:    store,
		BlockDir: blockDir,
		Addr:     addr,
	}
}

// Start starts the HTTP server (blocking).
func (s *Server) Start() {
	http.HandleFunc("/state/land/", s.handleLand)
	http.HandleFunc("/state/player/", s.handlePlayer)
	http.HandleFunc("/lastRoot", s.handleLastRoot)
	http.HandleFunc("/blocks/", s.handleBlock)      // /blocks/<n>
	http.HandleFunc("/blocks/latest", s.handleLast) // latest block
	http.HandleFunc("/tx", s.handleTx)              // POST tx

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

// ---------------- Handlers ----------------

func (s *Server) handleLand(w http.ResponseWriter, r *http.Request) {
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
	// placeholder: no SMT yet — return snapshot size + dummy root
	snap := s.Store.GetSnapshot()
	resp := map[string]interface{}{
		"keys": len(snap),
		"root": "dummy-root",
	}
	writeJSON(w, resp)
}

// GET /blocks/<n> where <n> is numeric (e.g. 000001) or filename suffix (e.g. 1 or 000001).
// The caller should use the same formatting the leader saves files in (block-%06d.json).
func (s *Server) handleBlock(w http.ResponseWriter, r *http.Request) {
	trim := strings.TrimPrefix(r.URL.Path, "/blocks/")
	if trim == "" {
		http.Error(w, "missing block id", http.StatusBadRequest)
		return
	}
	// support "latest" routed elsewhere; here we expect number
	// try to interpret as integer first
	if trim == "latest" {
		s.handleLast(w, r)
		return
	}

	// accept both plain numbers and padded numbers
	n, err := strconv.Atoi(trim)
	if err == nil {
		filename := fmt.Sprintf("block-%06d.json", n)
		path := filepath.Join(s.BlockDir, filename)
		serveFileOr404(w, path)
		return
	}

	// otherwise try exact filename
	path := filepath.Join(s.BlockDir, fmt.Sprintf("block-%s.json", trim))
	serveFileOr404(w, path)
}

func (s *Server) handleLast(w http.ResponseWriter, r *http.Request) {
	entries, err := os.ReadDir(s.BlockDir)
	if err != nil || len(entries) == 0 {
		http.Error(w, "no blocks available", http.StatusNotFound)
		return
	}
	// find highest block-*.json by parsing heights
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

// POST /tx
// Accepts a JSON transaction (types.Transaction). If tx.ID is empty it will be computed.
// Publishes the tx to node.TxTopic (so leader will pick it up the same as pubsub clients).
func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) {
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

	// compute and set ID if not present
	if strings.TrimSpace(tx.ID) == "" {
		tx.ID = computeTxID(&tx)
	}

	// publish to network TxTopic
	msg := network.TxMessage{Tx: &tx}
	data, err := network.EncodeTx(msg)
	if err != nil {
		http.Error(w, "failed to encode tx", http.StatusInternalServerError)
		return
	}

	// publish using Node's TxTopic
	if s.Node == nil || s.Node.TxTopic == nil {
		http.Error(w, "node tx topic not available on server", http.StatusInternalServerError)
		return
	}
	// use background context for publish
	if err := s.Node.TxTopic.Publish(context.Background(), data); err != nil {
		http.Error(w, fmt.Sprintf("failed to publish tx: %v", err), http.StatusInternalServerError)
		return
	}

	resp := map[string]string{"status": "submitted", "txid": tx.ID}
	writeJSON(w, resp)
}

// ---------------- Helpers ----------------

func serveFileOr404(w http.ResponseWriter, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		http.Error(w, "block not found", http.StatusNotFound)
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// computeTxID deterministically creates an ID for a transaction.
// It sorts payload keys for determinism and returns the hex sha256.
func computeTxID(tx *types.Transaction) string {
	h := sha256.New()
	h.Write([]byte(tx.Sender))
	h.Write([]byte("|"))
	h.Write([]byte(fmt.Sprintf("%d", tx.Nonce)))
	h.Write([]byte("|"))
	h.Write([]byte(tx.Action))
	h.Write([]byte("|"))

	// deterministic serialization of payload map
	if tx.Payload != nil {
		keys := make([]string, 0, len(tx.Payload))
		for k := range tx.Payload {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.Write([]byte(k))
			h.Write([]byte("="))
			h.Write([]byte(tx.Payload[k]))
			h.Write([]byte(";"))
		}
	}

	// include pubkey+sig length to differentiate
	if len(tx.PubKey) > 0 {
		h.Write(tx.PubKey)
	}
	if len(tx.Sig) > 0 {
		h.Write(tx.Sig)
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}
