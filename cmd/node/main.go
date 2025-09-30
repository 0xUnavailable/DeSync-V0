package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MelzLabs/DeSync/api"
	"github.com/MelzLabs/DeSync/consensus"
	"github.com/MelzLabs/DeSync/network"
)

var (
	roleFlag        = flag.String("role", "worker", "node role: leader or worker")
	idFlag          = flag.String("id", "", "node id (required)")
	portFlag        = flag.String("port", "9000", "listen port")
	connectFlag     = flag.String("connect", "", "multiaddr to connect (optional)")
	keyfileFlag     = flag.String("keyfile", "", "keyfile for identity (optional)")
	batchSizeFlag   = flag.Int("batchsize", 1000, "batch size for leader")
	batchInterval   = flag.Duration("batchinterval", 2*time.Second, "batch flush interval")
	blockInterval   = flag.Duration("blockinterval", 10*time.Second, "block creation interval")
	queueInterval   = flag.Duration("queuemon", 5*time.Second, "queue monitor interval")
	apiPortFlag     = flag.String("apiport", "8080", "port for HTTP API (leader only)")
	monthBlocksFlag = flag.Int("monthblocks", 10, "number of blocks per month (game loop)")
	staticDirFlag   = flag.String("staticdir", "", "directory to serve static frontend files (optional)")
	worldSizeFlag   = flag.Int("worldsize", 12, "world grid size (NxN plots) for leader to generate")
	initWorldFlag   = flag.Bool("initworld", false, "populate world with pseudo state for testing (leader only)")
)

func main() {
	flag.Parse()

	if *idFlag == "" {
		log.Fatal("need --id flag")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load or create key
	priv, err := network.LoadOrCreatePrivKey(*keyfileFlag)
	if err != nil {
		log.Fatalf("failed to load/create key: %v", err)
	}
	addr := "/ip4/0.0.0.0/tcp/" + *portFlag
	node, err := network.NewNodeWithKey(ctx, addr, priv)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}
	log.Printf("Node started with peer ID: %s, listening on %s", node.Host.ID().String(), addr)

	// optional bootstrap connect
	if *connectFlag != "" {
		if err := node.Connect(ctx, *connectFlag); err != nil {
			log.Printf("Warning: failed to connect to %s: %v", *connectFlag, err)
		}
	}

	switch *roleFlag {
	case "leader":
		startLeader(ctx, node, *worldSizeFlag, *initWorldFlag)
	case "worker":
		startWorker(ctx, node)
	default:
		log.Fatalf("unknown role: %s", *roleFlag)
	}

	// wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down node...")
}

func startLeader(ctx context.Context, node *network.Node, worldSize int, initWorld bool) {
	leader := consensus.NewLeader(*idFlag)
	leader.NetNode = node

	// Initialize world grid only if flag is set
	if initWorld {
		leader.InitWorld(worldSize)
		log.Printf("Leader %s: initialized world grid (%dx%d plots)", *idFlag, worldSize, worldSize)
	}

	if err := leader.StartWorkerRegistry(ctx); err != nil {
		log.Fatalf("StartWorkerRegistry failed: %v", err)
	}
	if err := leader.StartTxListener(ctx); err != nil {
		log.Fatalf("StartTxListener failed: %v", err)
	}
	if err := leader.ListenResults(ctx); err != nil {
		log.Fatalf("ListenResults failed: %v", err)
	}

	// run leader loops
	go leader.RunBatchLoop(ctx, *batchSizeFlag, *batchInterval)
	go leader.StartQueueMonitor(ctx, *queueInterval)
	go leader.BlockTicker(ctx, *blockInterval, *monthBlocksFlag)

	// ensure block dir exists
	blockDir := "blocks"
	_ = os.MkdirAll(blockDir, 0755)

	// static frontend dir
	staticDir := *staticDirFlag
	if staticDir == "" {
		staticDir = "ui"
	}

	// start HTTP API (explorer + tx submit)
	apiServer := api.NewServer(node, leader.State, blockDir, ":"+*apiPortFlag, staticDir)
	go apiServer.Start()

	log.Printf("Node %s started as leader with API on :%s (monthblocks=%d, staticdir=%s, initworld=%v)",
		*idFlag, *apiPortFlag, *monthBlocksFlag, staticDir, initWorld)
}

func startWorker(ctx context.Context, node *network.Node) {
	worker := consensus.NewWorker(*idFlag, node)
	if err := worker.Start(ctx); err != nil {
		log.Fatalf("worker start failed: %v", err)
	}
	log.Printf("Node %s started as worker", *idFlag)
}