package types

import "time"

// Block represents a committed block saved to disk.
type Block struct {
	Height  int             `json:"height"`
	Txs     []*Transaction  `json:"txs"`
	Root    []byte          `json:"root"`
	Time    time.Time       `json:"time"`
}
