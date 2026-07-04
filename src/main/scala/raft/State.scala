package raft

/** The three possible roles a Raft node can occupy. */
enum Role:
  case Follower, Candidate, Leader

/**
 * Persistent state that must survive restarts.
 *
 * @param currentTerm  latest term the server has seen
 * @param votedFor     candidateId that received vote in current term (or None)
 * @param log          the replicated log entries
 */
final case class PersistentState(
    currentTerm: Int = 0,
    votedFor: Option[String] = None,
    log: Vector[LogEntry] = Vector.empty
)

/**
 * Volatile state maintained on every server.
 *
 * @param commitIndex  index of highest log entry known to be committed
 * @param lastApplied  index of highest log entry applied to state machine
 */
final case class VolatileState(
    commitIndex: Int = 0,
    lastApplied: Int = 0
)

/**
 * Volatile state maintained only on leaders.
 *
 * @param nextIndex   for each server, index of the next log entry to send
 * @param matchIndex  for each server, index of highest log entry known replicated
 */
final case class LeaderVolatileState(
    nextIndex: Map[String, Int] = Map.empty,
    matchIndex: Map[String, Int] = Map.empty
)

/**
 * A snapshot of the state machine for log compaction.
 *
 * @param lastIncludedIndex  the last log index included in the snapshot
 * @param lastIncludedTerm   the term of lastIncludedIndex
 * @param data               the serialised state machine state
 */
final case class Snapshot(
    lastIncludedIndex: Int,
    lastIncludedTerm: Int,
    data: Map[String, String]
)

/**
 * Aggregate node state combining all Raft state components.
 */
final case class NodeState(
    role: Role = Role.Follower,
    persistent: PersistentState = PersistentState(),
    volatile: VolatileState = VolatileState(),
    leaderState: Option[LeaderVolatileState] = None,
    snapshot: Option[Snapshot] = None,
    stateMachine: Map[String, String] = Map.empty,
    votesReceived: Set[String] = Set.empty,
    currentLeader: Option[String] = None
):
  def term: Int = persistent.currentTerm
  def log: Vector[LogEntry] = persistent.log

  /** Index of the last entry in the log (1-based; 0 means empty). */
  def lastLogIndex: Int =
    snapshot match
      case Some(s) => s.lastIncludedIndex + persistent.log.size
      case None    => persistent.log.size

  /** Term of the last entry in the log. */
  def lastLogTerm: Int =
    if persistent.log.nonEmpty then persistent.log.last.term
    else snapshot.map(_.lastIncludedTerm).getOrElse(0)

  /** Retrieve a log entry by its 1-based global index. */
  def logEntry(index: Int): Option[LogEntry] =
    val offset = snapshot.map(_.lastIncludedIndex).getOrElse(0)
    val localIndex = index - offset - 1
    if localIndex >= 0 && localIndex < persistent.log.size then
      Some(persistent.log(localIndex))
    else None

  /** Term of the entry at the given 1-based global index. */
  def termAt(index: Int): Int =
    if index <= 0 then 0
    else
      snapshot match
        case Some(s) if index == s.lastIncludedIndex => s.lastIncludedTerm
        case Some(s) if index < s.lastIncludedIndex  => 0
        case _ => logEntry(index).map(_.term).getOrElse(0)
