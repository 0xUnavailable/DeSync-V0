package network

import (
	"context"
	"encoding/hex"
	"log"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Node wraps libp2p host and pubsub topics.
type Node struct {
	Host         host.Host
	PubSub       *pubsub.PubSub
	WorkersTopic *pubsub.Topic
	TxTopic      *pubsub.Topic
	BatchTopic   *pubsub.Topic
	ResultTopic  *pubsub.Topic
	LeaderTopic  *pubsub.Topic
}



// NewNodeWithKey starts a libp2p node with given privkey
func NewNodeWithKey(ctx context.Context, listenAddr string, priv crypto.PrivKey) (*Node, error) {
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.Identity(priv),
	)
	if err != nil {
		return nil, err
	}

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, err
	}

	workersTopic, _ := ps.Join("workers")
	txTopic, _ := ps.Join("txs")
	batchTopic, _ := ps.Join("batches")
	resultTopic, _ := ps.Join("results")
	leaderTopic, _ := ps.Join("leader")

	n := &Node{
		Host:         h,
		PubSub:       ps,
		WorkersTopic: workersTopic,
		TxTopic:      txTopic,
		BatchTopic:   batchTopic,
		ResultTopic:  resultTopic,
		LeaderTopic:  leaderTopic,
	}

	// MDNS discovery (local dev)
	ser := mdns.NewMdnsService(h, "desync-mdns", &discoveryNotifee{h: h})
	if err != nil {
		log.Printf("mdns disabled: %v", err)
	} else {
		_ = ser
	}

	log.Printf("Node started with peer ID: %s, listening on %s", h.ID().String(), listenAddr)
	return n, nil
}

type discoveryNotifee struct {
	h host.Host
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.h.ID() {
		return
	}
	if err := n.h.Connect(context.Background(), pi); err == nil {
		log.Printf("Connected to peer %s", pi.ID)
	}
}

// Connect to a bootstrap peer
func (n *Node) Connect(ctx context.Context, addr string) error {
	pi, err := peer.AddrInfoFromString(addr)
	if err != nil {
		return err
	}
	if err := n.Host.Connect(ctx, *pi); err != nil {
		return err
	}
	log.Printf("Connected to %s", pi.ID.String())
	return nil
}

// GetPeerIDHex returns peer id as hex
func (n *Node) GetPeerIDHex() string {
	return hex.EncodeToString([]byte(n.Host.ID()))
}
