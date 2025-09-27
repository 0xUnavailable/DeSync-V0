# Makefile for DeSync

.PHONY: all list leader worker1 worker2 worker3 workers loadgen loadgen_http inspect demo clean get-leader-id frontend

# Default
all: leader worker1

# List available targets
list:
	@echo "Available targets:"
	@echo "  leader        - Start leader node (with API + Explorer UI on :8080)"
	@echo "  worker1       - Start worker1 (port 9001)"
	@echo "  worker2       - Start worker2 (port 9002)"
	@echo "  worker3       - Start worker3 (port 9003)"
	@echo "  workers       - Start worker1, worker2, worker3"
	@echo "  loadgen       - Run libp2p-based load generator (100k tx)"
	@echo "  loadgen_http  - Run HTTP-based load generator (100k tx)"
	@echo "  inspect       - Inspect committed blocks in ./blocks"
	@echo "  demo          - Launch leader, 3 workers, and HTTP loadgen"
	@echo "  get-leader-id - Get the leader's peer ID from running process"
	@echo "  frontend      - Copy/update frontend files into ./frontend"
	@echo "  clean         - Remove blocks/ and *.key files"

# Leader node (with API + monthblocks + static frontend)
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
		--apiport=8080 \
		--monthblocks=10 \
		--staticdir=ui

# Helper target to get leader ID from a running leader
get-leader-id:
	@echo "Attempting to get leader ID from HTTP API..."
	@curl -s http://127.0.0.1:8080/status 2>/dev/null | grep -o '"peer_id":"[^"]*"' | cut -d'"' -f4 || \
	(echo "Could not get leader ID via API. Make sure leader is running on port 8080." && \
	 echo "You can also set LEADER_ID manually:" && \
	 echo "  LEADER_ID=12D3KooW... make worker1" && exit 1)

# Worker nodes with automatic leader discovery
worker1:
	@if [ -z "$(LEADER_ID)" ]; then \
		echo "LEADER_ID not set. Attempting to discover..."; \
		DISCOVERED_ID=$$(curl -s http://127.0.0.1:8080/status 2>/dev/null | grep -o '"peer_id":"[^"]*"' | cut -d'"' -f4); \
		if [ -z "$$DISCOVERED_ID" ]; then \
			echo "Error: Could not discover leader ID. Please:"; \
			echo "  1. Make sure leader is running: make leader"; \
			echo "  2. Set LEADER_ID manually: LEADER_ID=12D3KooW... make worker1"; \
			exit 1; \
		else \
			echo "Discovered leader ID: $$DISCOVERED_ID"; \
			go run ./cmd/node \
				--role=worker \
				--id=worker1 \
				--port=9001 \
				--keyfile=worker1.key \
				--connect=/ip4/127.0.0.1/tcp/9000/p2p/$$DISCOVERED_ID; \
		fi; \
	else \
		echo "Using provided LEADER_ID: $(LEADER_ID)"; \
		go run ./cmd/node \
			--role=worker \
			--id=worker1 \
			--port=9001 \
			--keyfile=worker1.key \
			--connect=/ip4/127.0.0.1/tcp/9000/p2p/$(LEADER_ID); \
	fi

worker2:
	@if [ -z "$(LEADER_ID)" ]; then \
		echo "LEADER_ID not set. Attempting to discover..."; \
		DISCOVERED_ID=$$(curl -s http://127.0.0.1:8080/status 2>/dev/null | grep -o '"peer_id":"[^"]*"' | cut -d'"' -f4); \
		if [ -z "$$DISCOVERED_ID" ]; then \
			echo "Error: Could not discover leader ID. Please:"; \
			echo "  1. Make sure leader is running: make leader"; \
			echo "  2. Set LEADER_ID manually: LEADER_ID=12D3KooW... make worker2"; \
			exit 1; \
		else \
			echo "Discovered leader ID: $$DISCOVERED_ID"; \
			go run ./cmd/node \
				--role=worker \
				--id=worker2 \
				--port=9002 \
				--keyfile=worker2.key \
				--connect=/ip4/127.0.0.1/tcp/9000/p2p/$$DISCOVERED_ID; \
		fi; \
	else \
		echo "Using provided LEADER_ID: $(LEADER_ID)"; \
		go run ./cmd/node \
			--role=worker \
			--id=worker2 \
			--port=9002 \
			--keyfile=worker2.key \
			--connect=/ip4/127.0.0.1/tcp/9000/p2p/$(LEADER_ID); \
	fi

worker3:
	@if [ -z "$(LEADER_ID)" ]; then \
		echo "LEADER_ID not set. Attempting to discover..."; \
		DISCOVERED_ID=$$(curl -s http://127.0.0.1:8080/status 2>/dev/null | grep -o '"peer_id":"[^"]*"' | cut -d'"' -f4); \
		if [ -z "$$DISCOVERED_ID" ]; then \
			echo "Error: Could not discover leader ID. Please:"; \
			echo "  1. Make sure leader is running: make leader"; \
			echo "  2. Set LEADER_ID manually: LEADER_ID=12D3KooW... make worker3"; \
			exit 1; \
		else \
			echo "Discovered leader ID: $$DISCOVERED_ID"; \
			go run ./cmd/node \
				--role=worker \
				--id=worker3 \
				--port=9003 \
				--keyfile=worker3.key \
				--connect=/ip4/127.0.0.1/tcp/9000/p2p/$$DISCOVERED_ID; \
		fi; \
	else \
		echo "Using provided LEADER_ID: $(LEADER_ID)"; \
		go run ./cmd/node \
			--role=worker \
			--id=worker3 \
			--port=9003 \
			--keyfile=worker3.key \
			--connect=/ip4/127.0.0.1/tcp/9000/p2p/$(LEADER_ID); \
	fi

workers: worker1 worker2 worker3

# Load generators
loadgen:
	@if [ -z "$(LEADER_ID)" ]; then \
		echo "LEADER_ID not set. Attempting to discover..."; \
		DISCOVERED_ID=$$(curl -s http://127.0.0.1:8080/status 2>/dev/null | grep -o '"peer_id":"[^"]*"' | cut -d'"' -f4); \
		if [ -z "$$DISCOVERED_ID" ]; then \
			echo "Error: Could not discover leader ID. Please set LEADER_ID manually."; \
			exit 1; \
		else \
			echo "Discovered leader ID: $$DISCOVERED_ID"; \
			go run ./cmd/loadgen \
				--connect=/ip4/127.0.0.1/tcp/9000/p2p/$$DISCOVERED_ID \
				--workers=10 \
				--rate=1000 \
				--txcount=100000; \
		fi; \
	else \
		go run ./cmd/loadgen \
			--connect=/ip4/127.0.0.1/tcp/9000/p2p/$(LEADER_ID) \
			--workers=10 \
			--rate=1000 \
			--txcount=100000; \
	fi

loadgen_http:
	go run ./cmd/loadgen_http \
		--api=http://127.0.0.1:8080 \
		--workers=10 \
		--rate=1000 \
		--txcount=100000

# Inspect committed blocks
inspect:
	go run ./cmd/inspect --dir=blocks

# Demo target (leader + 3 workers + HTTP loadgen + frontend)
demo:
	@echo ">>> Starting leader in background..."
	@LEADER_LOG=leader.log && \
	(go run ./cmd/node \
	  --role=leader \
	  --id=leader1 \
	  --port=9000 \
	  --keyfile=leader1.key \
	  --batchsize=1000 \
	  --batchinterval=2s \
	  --blockinterval=10s \
	  --queuemon=5s \
	  --apiport=8080 \
	  --monthblocks=10 \
	  --staticdir=ui> $$LEADER_LOG 2>&1 & echo $$! > leader.pid)
	@sleep 3
	@LEADER_ID=$$(grep "peer ID" leader.log | tail -1 | awk '{print $$7}' | sed 's/,//') && \
	echo ">>> Leader started with ID=$$LEADER_ID" && \
	echo ">>> Starting 3 workers..." && \
	(go run ./cmd/node --role=worker --id=worker1 --port=9001 --keyfile=worker1.key --connect=/ip4/127.0.0.1/tcp/9000/p2p/$$LEADER_ID & \
	 go run ./cmd/node --role=worker --id=worker2 --port=9002 --keyfile=worker2.key --connect=/ip4/127.0.0.1/tcp/9000/p2p/$$LEADER_ID & \
	 go run ./cmd/node --role=worker --id=worker3 --port=9003 --keyfile=worker3.key --connect=/ip4/127.0.0.1/tcp/9000/p2p/$$LEADER_ID &)
	@sleep 5
	@echo ">>> Running HTTP loadgen..."
	go run ./cmd/loadgen_http --api=http://127.0.0.1:8080 --workers=10 --rate=1000 --txcount=100000
	@echo ">>> Open Explorer at: http://127.0.0.1:8080/explorer.html"

# Frontend build/copy (if you want to sync static assets)
frontend:
	@mkdir -p frontend
	@cp -u explorer.html frontend/ || true

# Clean build artifacts
clean:
	rm -rf blocks *.key *.log *.pid 
