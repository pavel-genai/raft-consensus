# Raft Consensus

A Scala 3 implementation of the Raft consensus protocol using Akka actors.

## Features

- **Leader Election** with randomized timeouts
- **Log Replication** via AppendEntries RPCs
- **Commit Index Advancement** with majority acknowledgment
- **Log Compaction** via snapshots
- **5-node cluster** simulation in a single JVM
- **CLI** for sending key-value commands

## Running

```bash
sbt run
```

## Testing

```bash
sbt test
```

## Structure

- `src/main/scala/raft/Node.scala` — Raft node actor
- `src/main/scala/raft/State.scala` — node state management
- `src/main/scala/raft/Log.scala` — replicated log
- `src/main/scala/raft/Message.scala` — Raft RPC messages
- `src/main/scala/raft/Cluster.scala` — cluster management
- `src/main/scala/raft/Main.scala` — entry point and CLI
