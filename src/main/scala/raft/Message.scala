package raft

import org.apache.pekko.actor.typed.ActorRef

/** All messages that a Raft node can receive. */
sealed trait RaftMessage

object RaftMessage:

  // --- RPC requests ---

  /**
   * RequestVote RPC (invoked by candidates to gather votes).
   */
  final case class RequestVote(
      term: Int,
      candidateId: String,
      lastLogIndex: Int,
      lastLogTerm: Int,
      replyTo: ActorRef[RaftMessage]
  ) extends RaftMessage

  /**
   * RequestVote RPC response.
   */
  final case class RequestVoteResult(
      term: Int,
      voteGranted: Boolean,
      voterId: String
  ) extends RaftMessage

  /**
   * AppendEntries RPC (invoked by leader to replicate log entries and as heartbeat).
   */
  final case class AppendEntries(
      term: Int,
      leaderId: String,
      prevLogIndex: Int,
      prevLogTerm: Int,
      entries: Vector[LogEntry],
      leaderCommit: Int,
      replyTo: ActorRef[RaftMessage]
  ) extends RaftMessage

  /**
   * AppendEntries RPC response.
   */
  final case class AppendEntriesResult(
      term: Int,
      success: Boolean,
      followerId: String,
      matchIndex: Int
  ) extends RaftMessage

  /**
   * InstallSnapshot RPC (sent by leader when a follower is too far behind).
   */
  final case class InstallSnapshot(
      term: Int,
      leaderId: String,
      snapshot: Snapshot,
      replyTo: ActorRef[RaftMessage]
  ) extends RaftMessage

  final case class InstallSnapshotResult(
      term: Int,
      followerId: String
  ) extends RaftMessage

  // --- Internal timers ---

  /** Fires when the election timeout elapses without hearing from a leader. */
  case object ElectionTimeout extends RaftMessage

  /** Fires periodically on the leader to send heartbeats. */
  case object HeartbeatTick extends RaftMessage

  /** Fires periodically to check if log compaction is needed. */
  case object CompactionTick extends RaftMessage

  // --- Client interaction ---

  /**
   * A client command to be replicated.
   */
  final case class ClientRequest(
      command: Command,
      replyTo: ActorRef[ClientResponse]
  ) extends RaftMessage

  sealed trait ClientResponse
  final case class ClientOk(result: Option[String] = None) extends ClientResponse
  final case class ClientRedirect(leaderId: Option[String]) extends ClientResponse
  final case class ClientError(message: String) extends ClientResponse

  /**
   * Query the current state machine value for a key (leader-only, linearisable read).
   */
  final case class ClientQuery(
      key: String,
      replyTo: ActorRef[ClientResponse]
  ) extends RaftMessage

  // --- Cluster management ---

  /** Provide the node with references to its peers. */
  final case class SetPeers(peers: Map[String, ActorRef[RaftMessage]]) extends RaftMessage

  /** Request the node's current debug state (used for testing). */
  final case class GetState(replyTo: ActorRef[NodeStateResponse]) extends RaftMessage

  final case class NodeStateResponse(
      nodeId: String,
      role: Role,
      term: Int,
      commitIndex: Int,
      logSize: Int,
      stateMachine: Map[String, String],
      currentLeader: Option[String]
  )
