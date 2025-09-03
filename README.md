# DESYNC Development Document

## Version 1.0
**Purpose:** This document outlines the development plan for DESYNC, a decentralized sequencer architecture for Layer 2 scaling solutions. It incorporates the core concepts and verbatim overview provided by the developer to ensure alignment with the original vision. The goal is to proceed with implementation, focusing on a modified Raft-inspired consensus for distributed sequencing, while addressing centralization risks in current rollups like Arbitrum and Optimism.

## Introduction

### Problem Statement
Current Layer 2 rollup implementations, such as those in Arbitrum and Optimism, rely on centralized sequencers, introducing risks including transaction censorship, single points of failure, centralized MEV extraction, and trust in operators. DESYNC aims to decentralize this process through a rotating leadership model, parallel worker execution, and Byzantine fault-tolerant mechanisms, enabling higher throughput and security without sacrificing liveness.

### Project Goals
- Achieve decentralized transaction ordering and execution for L2 rollups.
- Ensure censorship resistance, fault tolerance (up to 1/3 Byzantine nodes), and fair MEV distribution.
- Target throughput competitive with centralized sequencers (e.g., 1000+ TPS).
- Integrate with existing L1 blockchains (e.g., Ethereum) via rollup-style bundling.
- Minimize coordination overhead by avoiding full global consensus.

### Scope
This document covers architecture, key components, implementation phases, risks, and next steps. Development will prioritize a minimum viable product (MVP) on a testnet, with iterative improvements based on simulations.

## Overview 
> DESYNC  
> Utilizes a modified Raft consensus to coordinate sequencers to be distributed and decentralized  
> 
> IT WORKS LIKE THIS  
> There is a leadership model that works similarly to how raft works, but here there will be a leader and a backup leader, each election will be done every epoch, an epoch is to last 3 minutes, the process of an election works like this every node will use a commit-reveal process to generate the lowest possible hash using a VRF (VERIFIABLE RANDOM FUNCTION), the node with the lowest hash is assigned leader, while the next lowest hash is assigned backup leader  
> 
> For the transaction processing  
> The assigned leaders job is to receive the transactions as they arrive (sequential ordering), the keep record of the ordering, then distributes them to the worker nodes (everyone apart from the leader), this is done sequentially, and the transaction ordering are respected, after each worker receives their own batch, processing follows, when each worker finishes the transactions are submitted back to the leader to order back, using the initial ordering  
> 
> Each node in the network will have a copy of the epoch metadata similarly to how raft operates  
> the metadata entails the following  
> {  
> current_epoch,  
> leader,  
> backup_leader,  
> 
> }  
> A heartbeat will be used to keep track of leader activeness, if a leader becomes unresponsive, the backup takes over, in case of a scenario where both leaders are unresponsive (God forbid) an emergency election will occur, this signals the start of a new epoch  
> 
> The election process will go like this, 2 minutes for processing, 1 minute for commit-reveal  
> 
> Leader handoff will be done at the end of an epoch  
> 
> Suggestion:  
> Seems i will utilize proof of history as a method to keep track of time and the epoch, using a centralized system to keep track of time is a point of failure and can be gamed  
> 
> Measure to be taken :  
> 1. For a valid election there must be at least 2/3 of nodes for a valid participation  
> 2. To prevent sybiling of nodes, a combination of POS (proof of stake) and Commit-Reveal will be utilized, each node on the network will have to stake tokens as collateral, any penalty incurred will result to slashing of those tokens  
> 3. Penalties for failure to participate in elections, leader unresponsiveness, failure to reveal after committing during election will all result to slashing of staked token  
> 4. Leaders will receive incentives by claiming excess fees during transaction execution  
> 5. Gossip protocol will be used by both the leader and workers  to pass across information, first the leader uses it by passing across the ordering of transactions that were split across the workers, so the workers use this info to execute the transaction deterministically, and handle interdependent transactions for a visual it works like this  
> Leader receives 1000 transactions, leader keeps track of the order, leader splits the 1000 transactions evenly across the workers, if we have 5 workers, each worker gets 200 transactions, the order is maintained ie 1-200, 201-400, 401-600, 601-800, 801-1000,   
> Say there is a case of an interdependent transaction that happen to fall to different workers, this dilemma can easily be resolved, since the transactions are split across sequential workers, so if transaction 801 depend on the state of transaction 800, there is no way the 5 worker can know what the state is, this is where the gossip protocol come in handy, both workers can gossip state changes if a dilemma occurs  
> 6. Byzantine tolerance will be 1/3 of nodes, minimum election participation will be 2/3 of nodes  
> 7. Transactions are bundled by the leader and sent to the L1 for submission (rollup style)   
> 8. Each worker will have to verify they executed a certain batch of transaction, as a way of signing and verification   
> 9. Optimistic system will be used for challenges   
> 10. No global consensus is needed  
> 11. Workers (lol i dont know how to reward you guys, maybe try to win an election, lmao )  
> here is the full overview in my words

This overview serves as the foundational blueprint for development.

## Architecture Overview
DESYNC builds on a leader-worker model inspired by Raft consensus but modified for blockchain environments. It uses epochs for leadership rotation, gossip protocols for communication, and optimistic execution with challenges for efficiency.

### Key Components
- **Leadership Election**: Epoch-based (3 minutes: 2 min processing, 1 min election). Nodes stake via PoS and use commit-reveal with VRF to generate hashes. Lowest hash becomes leader, next lowest backup. Requires 2/3 participation; slashing for failures.
- **Transaction Processing Pipeline**: Leader orders transactions by arrival, splits into batches (e.g., even distribution while preserving order), distributes via gossip. Workers execute batches deterministically, gossip state changes for dependencies, sign results, and return to leader for bundling and L1 submission.
- **State Management**: All nodes maintain epoch metadata (current_epoch, leader, backup_leader). Heartbeats monitor leader; failover to backup or emergency election if both unresponsive.
- **Timekeeping**: Integrate Proof of History (PoH) for decentralized epoch timing. PoH uses a Verifiable Delay Function (VDF) to create a chain of sequential hashes, proving time passage without relying on external clocks. This generates "ticks" that nodes can verify publicly, preventing gaming and ensuring synchronized epochs.
- **Consensus and Verification**: No global consensus; workers sign executions, leader aggregates. Optimistic challenges via fraud proofs for disputes.
- **Incentives and Penalties**: Leaders claim excess fees; workers incentivized indirectly (e.g., election wins). Slashing for malice or unresponsiveness. Future: Add worker fee shares.
- **Fault Tolerance**: 1/3 Byzantine tolerance, gossip resilience to churn.

### Flow Diagram (Conceptual)
```
Transaction Submission → Leader Ordering & Batch Split → Gossip Distribution to Workers → Parallel Execution (with Gossip for Dependencies) → Worker Signing & Return → Leader Bundling → L1 Commitment
```
Epoch Cycle: Processing (2 min) → Election (1 min Commit-Reveal) → Handoff.

## Technical Details
- **VRF Commit-Reveal for Election**: Nodes commit hashed VRF outputs, reveal to determine lowest hashes. Inspired by systems like Algorand or Chainlink VRF for randomness. Ensures unpredictability and verifiability.
- **Gossip Protocol**: Use libp2p or similar for P2P propagation of batches and state changes. Handles dependencies by on-demand gossip (e.g., worker queries prior state if needed).
- **PoH Integration**: Nodes run local PoH generators (sequential SHA-256 hashing with VDF delays) to timestamp epochs. Sync via broadcast of PoH proofs; verify to agree on epoch boundaries.
- **L1 Integration**: Bundle as calldata/blobs on Ethereum; use BLS signatures for aggregated worker proofs.
- **Tech Stack**: Go for core (inspired by Solana/Substrate); EVM-compatible VM for execution; PoS via custom token or ERC-20.

## Implementation Roadmap
### Phase 1: Core Consensus and Elections
- Implement PoS staking and slashing.
- Develop commit-reveal VRF election with 2/3 threshold.
- Add heartbeat monitoring and failover.
- Integrate PoH for epoch timing (prototype VDF generator).
- Test: Simulate 100 nodes, measure election fairness.

### Phase 2: Transaction Processing and Gossip 
- Build leader batching and gossip distribution.
- Worker execution with signing and dependency gossip.
- Optimistic challenges via fraud proofs.
- Test: End-to-end tx flow, handle 10k txs with dependencies.

### Phase 3: L1 Integration and Incentives 
- Rollup bundling and L1 submission (e.g., Ethereum testnet).
- Fee claiming for leaders; explore worker rewards.
- Byzantine simulations (1/3 malicious nodes).
- Test: Deploy to Sepolia, benchmark TPS/latency.

### Phase 4: Auditing and Production 
- Security audit (focus on VRF, PoH, gossip).
- Cross-rollup APIs.
- Testnet deployment and node onboarding.
- Monitoring tools for liveness/MEV.

## Risks and Mitigations
- **Dependency Bottlenecks**: Mitigate with advanced grouping (e.g., static analysis pre-split).
- **Low Worker Participation**: Add fee splits or restaking.
- **PoH Sync Issues**: Fallback to hybrid timing if VDF delays vary.
- **Sybil Attacks**: Enforce minimum stake and KYC-optional verification.
- **Scalability**: Optimize gossip for 1000+ nodes; consider sharding later.

