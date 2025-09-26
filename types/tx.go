package types

import (
	"crypto/ed25519"
	"crypto/rand"
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
	PubKey  []byte
	Sig     []byte
}

func NewTxn(sender, action string, payload map[string]string) *Transaction {
	idb := make([]byte, 8)
	_, _ = rand.Read(idb)
	var payloadCopy map[string]string
	if payload != nil {
		payloadCopy = make(map[string]string, len(payload))
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
	t.PubKey = make([]byte, len(pk))
	copy(t.PubKey, pk)
	t.Sig = make([]byte, len(sig))
	copy(t.Sig, sig)
	return nil
}

func (t *Transaction) VerifySignature() bool {
	if len(t.PubKey) == 0 || len(t.Sig) == 0 {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(t.PubKey), t.MsgBytes(), t.Sig)
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
