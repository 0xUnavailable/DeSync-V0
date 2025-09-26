# ==== CONFIG ====
LEADER_ID = leader1
LEADER_PORT = 9000
LEADER_KEY = leader1.key
LEADER_BATCHSIZE = 1000
LEADER_BATCHINTERVAL = 2s
LEADER_BLOCKINTERVAL = 10s
LEADER_QUEUEMON = 5s

# Replace with actual leader peer-id after running `make leader`
LEADER_PEERID = 12D3KooWBWaGuEJ2iKfuyAWMzsCQjxAAD4d11tLQHAEpgCLi9icx

LOADGEN_WORKERS = 10
LOADGEN_RATE = 1000
LOADGEN_TXCOUNT = 100000

BLOCKS_DIR = blocks

# ==== TARGETS ====

.PHONY: all leader worker1 worker2 worker3 worker4 loadgen inspect clean

all: leader

leader:
	@echo "🚀 Starting leader..."
	go run ./cmd/node \
	  --role=leader \
	  --id=$(LEADER_ID) \
	  --port=$(LEADER_PORT) \
	  --keyfile=$(LEADER_KEY) \
	  --batchsize=$(LEADER_BATCHSIZE) \
	  --batchinterval=$(LEADER_BATCHINTERVAL) \
	  --blockinterval=$(LEADER_BLOCKINTERVAL) \
	  --queuemon=$(LEADER_QUEUEMON)

worker1:
	@echo "🚀 Starting worker1..."
	go run ./cmd/node \
	  --role=worker \
	  --id=worker1 \
	  --port=9001 \
	  --keyfile=worker1.key \
	  --connect=/ip4/127.0.0.1/tcp/$(LEADER_PORT)/p2p/$(LEADER_PEERID)

worker2:
	@echo "🚀 Starting worker2..."
	go run ./cmd/node \
	  --role=worker \
	  --id=worker2 \
	  --port=9002 \
	  --keyfile=worker2.key \
	  --connect=/ip4/127.0.0.1/tcp/$(LEADER_PORT)/p2p/$(LEADER_PEERID)

worker3:
	@echo "🚀 Starting worker3..."
	go run ./cmd/node \
	  --role=worker \
	  --id=worker3 \
	  --port=9003 \
	  --keyfile=worker3.key \
	  --connect=/ip4/127.0.0.1/tcp/$(LEADER_PORT)/p2p/$(LEADER_PEERID)

worker4:
	@echo "🚀 Starting worker4..."
	go run ./cmd/node \
	  --role=worker \
	  --id=worker4 \
	  --port=9004 \
	  --keyfile=worker4.key \
	  --connect=/ip4/127.0.0.1/tcp/$(LEADER_PORT)/p2p/$(LEADER_PEERID)

loadgen:
	@echo "🚀 Starting load generator..."
	go run ./cmd/loadgen \
	  --connect=/ip4/127.0.0.1/tcp/$(LEADER_PORT)/p2p/$(LEADER_PEERID) \
	  --workers=$(LOADGEN_WORKERS) \
	  --rate=$(LOADGEN_RATE) \
	  --txcount=$(LOADGEN_TXCOUNT)

inspect:
	@echo "🔎 Inspecting blocks in $(BLOCKS_DIR)..."
	go run ./cmd/inspect --dir=$(BLOCKS_DIR)

clean:
	@echo "🧹 Cleaning block data and keys..."
	rm -rf $(BLOCKS_DIR)
