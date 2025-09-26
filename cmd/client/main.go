package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/MelzLabs/DeSync/network"
	"github.com/MelzLabs/DeSync/types"
	"github.com/libp2p/go-libp2p/core/crypto"
)

func main() {
	connect := flag.String("connect", "", "multiaddr to connect")
	action := flag.String("action", "allocate", "action type (allocate, transfer, paytax, etc.)")
	args := flag.String("args", "{}", "json args for action (map[string]string in JSON)")
	sender := flag.String("sender", "player1", "sender id")
	flag.Parse()

	if *connect == "" {
		log.Fatal("need --connect to at least one node")
	}

	ctx := context.Background()

	// libp2p identity key
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

	// tx signing key (separate from libp2p identity)
	_, txPriv, _ := ed25519.GenerateKey(rand.Reader)

	// build payload
	var payload map[string]string
	_ = json.Unmarshal([]byte(*args), &payload)

	tx := types.NewTxn(*sender, *action, payload)
	tx.Nonce = uint64(time.Now().UnixNano())

	if err := tx.Sign(txPriv); err != nil {
		log.Fatal(err)
	}

	msg := network.TxMessage{Tx: tx}
	data, _ := network.EncodeTx(msg)
	_ = node.TxTopic.Publish(ctx, data)

	fmt.Printf("Submitted tx: id=%s action=%s sender=%s payload=%v\n",
		tx.ID, tx.Action, tx.Sender, tx.Payload)
}
