# DESYNC

**Decentralized Sequencer Architecture for Layer 2 Rollups - Minimum Viable Product**

DESYNC is a decentralized sequencer architecture for Layer 2 rollups, designed to eliminate centralized points of failure in transaction ordering. It uses a modified Raft-inspired consensus with rotating leadership, gossip protocols, and Proof of History (PoH) for secure, high-throughput transaction processing.

## Overview

### Purpose
Decentralize L2 sequencing to prevent:
- Censorship attacks
- Single points of failure
- Centralized MEV extraction

### Mechanism
A leader elected via VRF commit-reveal orders transactions, splits them across workers, and uses gossip for coordination. Workers execute deterministically, sign results, and return to the leader for L1 bundling.

### Key Features (MVP Target)
- **Epoch-based leadership**: 3-minute epochs (2 minutes processing, 1 minute election)
- **Proof of Stake**: Basic staking and slashing mechanisms
- **Proof of History**: Decentralized epoch timing using VDF prototype
- **Optimistic execution**: No global consensus; challenges handled via fraud proofs
- **Target throughput**: 1000+ TPS with 1/3 Byzantine fault tolerance
- **Fair MEV distribution**: Experimental approach to prevent centralized extraction
- **Censorship resistance**: Distributed transaction ordering
- **Ethereum integration**: Basic compatibility with Ethereum-style L1 blockchains

## MVP Development Roadmap

### Phase 1: Core Consensus and Elections (I AM HERE)
**Objectives:**
- Implement basic PoS staking and slashing
- Develop VRF commit-reveal election with 2/3 threshold
- Build heartbeat monitoring and failover
- Create PoH prototype for epoch timing (VDF implementation)

**Testing:**
- Simulate 100 nodes for election fairness validation

### Phase 2: Transaction Processing and Gossip
**Objectives:**
- Implement leader batching and gossip distribution
- Develop worker execution with signing and dependency gossip
- Build basic optimistic fraud proof system

**Testing:**
- Process 10,000 transactions with dependency handling

### Phase 3: L1 Integration and Incentives
**Objectives:**
- Implement rollup bundling for Ethereum testnet
- Develop leader fee claiming mechanism
- Explore basic worker reward systems
- Conduct Byzantine simulations (1/3 malicious nodes)

**Testing:**
- Deploy to Sepolia testnet
- Benchmark TPS and latency performance

### Phase 4: MVP Deployment
**Objectives:**
- Implement cross-rollup APIs
- Deploy to testnet with node onboarding
- Basic monitoring for liveness and MEV

**Deliverables:**
- Functional MVP on testnet
- Basic monitoring and metrics

## Current Status
## Technical Architecture (Draft)

- **Consensus**: Modified Raft-inspired with rotating leadership
- **Election**: VRF commit-reveal mechanism
- **Communication**: Gossip protocols for coordination
- **Timing**: Proof of History (PoH) using Verifiable Delay Functions
- **Security**: Basic Proof of Stake with slashing conditions
- **Target Performance**: 1000+ TPS (experimental)
- **Fault Tolerance**: Byzantine fault tolerance up to 1/3 of nodes

 