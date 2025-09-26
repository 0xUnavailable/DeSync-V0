# Makefile for DeSync

.PHONY: all list leader worker1 worker2 worker3 workers loadgen loadgen_http inspect clean

# Default
all: leader worker1

# List available targets
list:
	@echo "Available targets:"
	@echo "  leader        - Start leader node (with API on :8080)"
	@echo "  worker1       - Start worker1 (port 9001)"
	@echo "  worker2       - Start worker2 (port 9002)"
	@echo "  worker3       - Start worker3 (port 9003)"
	@echo "  workers       - Start worker1, worker2, worker3"
	@echo "  loadgen       - Run libp2p-based load generator (100k tx)"
	@echo "  loadgen_http  - Run HTTP-based load generator (100k tx)"
	@echo "  inspect       - Inspect committed blocks in ./blocks"
	@echo "  clean         - Remove blocks/ and *.key files"

# Build commands
leader:
	go run ./cmd/node \
		--role=leader \
		--id=leader1 \
		--port=9000 \
		--keyfile=leader1.key \
		--batchsize=1000 \
		--batchinterval=2s \
		--blockinterval=10s \
		--queuemon=5s \
		--apiport=8080

worker1:
	go run ./cmd/node \
		--role=worker \
		--id=worker1 \
		--port=9001 \
		--keyfile=worker1.key \
		--connect=/ip4/127.0.0.1/tcp/9000/p2p/$(LEADER_ID)

worker2:
	go run ./cmd/node \
		--role=worker \
		--id=worker2 \
		--port=9002 \
		--keyfile=worker2.key \
		--connect=/ip4/127.0.0.1/tcp/9000/p2p/$(LEADER_ID)

worker3:
	go run ./cmd/node \
		--role=worker \
		--id=worker3 \
		--port=9003 \
		--keyfile=worker3.key \
		--connect=/ip4/127.0.0.1/tcp/9000/p2p/$(LEADER_ID)

workers: worker1 worker2 worker3

# Load generators
loadgen:
	go run ./cmd/loadgen \
		--connect=/ip4/127.0.0.1/tcp/9000/p2p/$(LEADER_ID) \
		--workers=10 \
		--rate=1000 \
		--txcount=100000

loadgen_http:
	go run ./cmd/loadgen_http \
		--api=http://127.0.0.1:8080 \
		--workers=10 \
		--rate=1000 \
		--txcount=100000

# Inspect committed blocks
inspect:
	go run ./cmd/inspect --dir=blocks

# Clean build artifacts
clean:
	rm -rf blocks
