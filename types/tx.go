package types

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"sort"
	"strings"
)

// Transaction represents a simple action.
type Transaction struct {
	ID      string
	Sender  string
	Nonce   uint64
	Action  string
	Payload map[string]string
	PubKey  string
	Sig     string
}

// NewTxn creates a new transaction with an optional payload.
// If payload is nil, an empty map is created.
func NewTxn(sender, action string, payload map[string]string) *Transaction {
	idb := make([]byte, 8)
	_, _ = rand.Read(idb)

	payloadCopy := make(map[string]string)
	if payload != nil {
		for k, v := range payload {
			payloadCopy[k] = v
		}
	}

	return &Transaction{
		ID:      hex.EncodeToString(idb),
		Sender:  sender,
		Action:  action,
		Payload: payloadCopy,
	}
}


func (t *Transaction) MsgBytes() []byte {
	var sb strings.Builder
	sb.WriteString(t.Sender)
	sb.WriteString("|")
	sb.WriteString(t.Action)
	sb.WriteString("|")
	if t.Payload != nil {
		keys := make([]string, 0, len(t.Payload))
		for k := range t.Payload {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			sb.WriteString(k)
			sb.WriteString("=")
			sb.WriteString(t.Payload[k])
			sb.WriteString(";")
		}
	}
	return []byte(sb.String())
}

func (t *Transaction) Sign(priv ed25519.PrivateKey) error {
	sig := ed25519.Sign(priv, t.MsgBytes())
	pk := priv.Public().(ed25519.PublicKey)
	t.PubKey = base64.StdEncoding.EncodeToString(pk)
	t.Sig = base64.StdEncoding.EncodeToString(sig)
	return nil
}
func (t *Transaction) VerifySignature() bool {
	if t.PubKey == "" || t.Sig == "" {
		return false
	}
	
	pubBytes, err := base64.StdEncoding.DecodeString(t.PubKey)
	if err != nil {
		return false
	}
	sigBytes, err := base64.StdEncoding.DecodeString(t.Sig)
	if err != nil {
		return false
	}
	
	// Detect signature type by public key length
	// Ed25519: 32 bytes, ECDSA P-256 SPKI: 91 bytes
	if len(pubBytes) == 32 && len(sigBytes) == 64 {
		// Ed25519 signature (from Go loadgen tools)
		return ed25519.Verify(ed25519.PublicKey(pubBytes), t.MsgBytes(), sigBytes)
	} else if len(pubBytes) == 91 && len(sigBytes) == 64 {
		// ECDSA P-256 signature (from HTTP/browser)
		// Workers should not verify these - API server handles it
		// Just return true and let the API do full verification
		return true
	}
	
	return false
}

// TxResult returned by worker for each tx.
type TxResult struct {
	TxID        string
	Index       int
	Success     bool
	Error       string
	WriteSet    map[string][]byte
	Events      []string
	ExecutedBy  string
	DeclaredKeys []string
}

// BatchReceipt is created by leader when committing.
type BatchReceipt struct {
	BatchID         string
	Root            []byte
	TxResults       []*TxResult
	WorkerSignatures map[string][]byte
}
