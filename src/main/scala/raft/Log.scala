package raft

/**
 * A single entry in the replicated log.
 *
 * @param term     the term when the entry was received by the leader
 * @param index    1-based position in the log
 * @param command  the state-machine command to apply
 */
final case class LogEntry(
    term: Int,
    index: Int,
    command: Command
)

/** Commands that the state machine understands. */
enum Command:
  /** Set a key to a value. */
  case Put(key: String, value: String)
  /** Remove a key. */
  case Delete(key: String)
  /** A no-op entry used after leader election to commit entries from prior terms. */
  case Noop

object LogOps:

  /**
   * Append new entries to the log, resolving conflicts per the Raft rules:
   *   - If an existing entry conflicts with a new one (same index, different term),
   *     delete the existing entry and all that follow it.
   *   - Append any new entries not already in the log.
   *
   * @param existing       current log entries
   * @param prevLogIndex   index of entry immediately preceding new entries
   * @param newEntries     entries to append
   * @param snapshotOffset the lastIncludedIndex of the current snapshot (0 if none)
   * @return updated log vector
   */
  def appendEntries(
      existing: Vector[LogEntry],
      prevLogIndex: Int,
      newEntries: Vector[LogEntry],
      snapshotOffset: Int = 0
  ): Vector[LogEntry] =
    if newEntries.isEmpty then existing
    else
      val localPrev = prevLogIndex - snapshotOffset
      // Truncate any conflicting suffix
      val base = existing.take(localPrev)
      base ++ newEntries

  /**
   * Compact the log by discarding entries up to and including `lastIndex`.
   *
   * @param log            current log
   * @param lastIndex      the last index to compact away
   * @param snapshotOffset current snapshot offset (0 if none)
   * @return remaining log entries after compaction
   */
  def compact(
      log: Vector[LogEntry],
      lastIndex: Int,
      snapshotOffset: Int = 0
  ): Vector[LogEntry] =
    val localIndex = lastIndex - snapshotOffset
    if localIndex >= log.size then Vector.empty
    else if localIndex <= 0 then log
    else log.drop(localIndex)

  /**
   * Apply committed but not-yet-applied entries to the state machine.
   *
   * @param stateMachine current state
   * @param log          the log
   * @param lastApplied  index of last applied entry
   * @param commitIndex  index of last committed entry
   * @param snapshotOffset current snapshot offset
   * @return (updated state machine, new lastApplied)
   */
  def applyCommitted(
      stateMachine: Map[String, String],
      log: Vector[LogEntry],
      lastApplied: Int,
      commitIndex: Int,
      snapshotOffset: Int = 0
  ): (Map[String, String], Int) =
    var sm = stateMachine
    var applied = lastApplied
    while applied < commitIndex do
      applied += 1
      val localIdx = applied - snapshotOffset - 1
      if localIdx >= 0 && localIdx < log.size then
        log(localIdx).command match
          case Command.Put(k, v) => sm = sm.updated(k, v)
          case Command.Delete(k) => sm = sm.removed(k)
          case Command.Noop      => // nothing
    (sm, applied)
