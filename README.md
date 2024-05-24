### CSE530 Winter 2024 - Distributed Systems Assignment 2 Readme

This repository contains the implementation of a modified Raft system with leader lease for a geo-distributed database cluster. Below is a guide on the project structure, implementation details, and test case scenarios.

---

### Project Structure

---
assignment/
│
├─ logs_node_0/
│  ├─ logs.txt
│  ├─ metadata.txt
│  ├─ dump.txt
│
├─ logs_node_1/
│  ├─ logs.txt
│  ├─ metadata.txt
│  ├─ dump.txt
│
└─ README.md

---

### Resources

- **Raft Algorithm:** [Original Paper](link) | [Medium Explanations Part 1](link), [Part 2](link) | [Raft Visualization](link)
- **Leader Leases:** [Yugabyte Article](link)

### Introduction

This assignment focuses on implementing a modified Raft system similar to those used by geo-distributed Database clusters such as CockroachDB or YugabyteDB. Raft is a consensus algorithm designed for distributed systems to ensure fault tolerance and consistency.

### Raft Modification (for faster Reads)

Traditionally, Raft requires the leader to exchange a heartbeat with a majority of peers before responding to a read request. Leader lease modification introduces time-based lease for leadership, enhancing read efficiency.

### Implementation Details

1. **Overview:** Implement Raft with leader lease. Each node is a process hosted on a separate VM on Google Cloud. Communication between nodes is via gRPC or ZeroMQ.

2. **Storage and Database Operations:** Key-value pairs stored in a fault-tolerant manner. Logs persisted in human-readable format.

3. **Client Interaction:** Clients interact with the Raft cluster to perform SET and GET operations. System handles leader changes gracefully.

4. **Standard Raft RPCs:** AppendEntry and RequestVote RPCs modified to include lease duration information.

5. **Election Functionalities:** Nodes participate in leader elections based on randomized timeouts. New leader waits for old leader's lease to timeout.

6. **Log Replication:** Logs replicated across nodes using AppendEntries RPC.

### Test Case Scenarios

1. Start the cluster with 5 nodes, wait for leader election and NO-OP entry to be appended in all logs.

2. Perform SET and GET requests.

3. Terminate follower nodes, perform SET and GET requests, and restart them.

4. Terminate current leader, wait for new leader election, perform requests, and restart old leader.

5. Terminate majority follower nodes, send requests before lease expiration, observe leader stepping down.

6. Terminate all nodes except two followers, send requests, observe absence of leader.

### Assumptions

- Only one cluster of the database is needed using Raft.
- Interrupts used to stop a node/program.


### for more deatils check this
https://docs.google.com/document/d/e/2PACX-1vSy3psit4UbAQci5vZhj-NhRQCIm5eBRdCDIXnnAe_cNPouWomX6P95MSuZdoSMd_rV0ugUJvJaVnGY/pub
