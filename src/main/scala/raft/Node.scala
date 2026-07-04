package raft

import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import scala.concurrent.duration.*
import scala.util.Random

/**
 * A single Raft node implemented as a Pekko Typed actor.
 *
 * The node transitions between Follower, Candidate, and Leader roles
 * according to the Raft protocol.
 */
object Node:

  private val HeartbeatInterval     = 150.millis
  private val ElectionTimeoutMin    = 300
  private val ElectionTimeoutMax    = 600
  private val CompactionInterval    = 5.seconds
  private val CompactionThreshold   = 50 // compact when log exceeds this many entries

  private case object ElectionTimerKey
  private case object HeartbeatTimerKey
  private case object CompactionTimerKey

  def apply(nodeId: String): Behavior[RaftMessage] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val node = NodeBehavior(nodeId, context, timers)
        node.start()
      }
    }

  private class NodeBehavior(
      nodeId: String,
      context: ActorContext[RaftMessage],
      timers: TimerScheduler[RaftMessage]
  ):
    private var state: NodeState = NodeState()
    private var peers: Map[String, ActorRef[RaftMessage]] = Map.empty
    // Track pending client requests so we can respond when entries are committed
    private var pendingRequests: Map[Int, ActorRef[RaftMessage.ClientResponse]] = Map.empty

    def start(): Behavior[RaftMessage] =
      resetElectionTimer()
      active()

    private def active(): Behavior[RaftMessage] =
      Behaviors.receiveMessage { msg =>
        handleMessage(msg)
        Behaviors.same
      }

    // ---- Timer management ----

    private def resetElectionTimer(): Unit =
      val timeout = ElectionTimeoutMin + Random.nextInt(ElectionTimeoutMax - ElectionTimeoutMin)
      timers.startSingleTimer(ElectionTimerKey, RaftMessage.ElectionTimeout, timeout.millis)

    private def startHeartbeatTimer(): Unit =
      timers.startTimerWithFixedDelay(HeartbeatTimerKey, RaftMessage.HeartbeatTick, HeartbeatInterval)

    private def stopHeartbeatTimer(): Unit =
      timers.cancel(HeartbeatTimerKey)

    private def startCompactionTimer(): Unit =
      timers.startTimerWithFixedDelay(CompactionTimerKey, RaftMessage.CompactionTick, CompactionInterval)

    private def stopCompactionTimer(): Unit =
      timers.cancel(CompactionTimerKey)

    // ---- Main message dispatch ----

    private def handleMessage(msg: RaftMessage): Unit = msg match
      case RaftMessage.SetPeers(p) =>
        peers = p
        context.log.info(s"[$nodeId] Peers set: ${p.keys.mkString(", ")}")

      case RaftMessage.GetState(replyTo) =>
        replyTo ! RaftMessage.NodeStateResponse(
          nodeId, state.role, state.term, state.volatile.commitIndex,
          state.lastLogIndex, state.stateMachine, state.currentLeader
        )

      case RaftMessage.ElectionTimeout =>
        handleElectionTimeout()

      case RaftMessage.HeartbeatTick =>
        if state.role == Role.Leader then sendHeartbeats()

      case RaftMessage.CompactionTick =>
        if state.role == Role.Leader then maybeCompact()

      case rv: RaftMessage.RequestVote =>
        handleRequestVote(rv)

      case rvr: RaftMessage.RequestVoteResult =>
        handleRequestVoteResult(rvr)

      case ae: RaftMessage.AppendEntries =>
        handleAppendEntries(ae)

      case aer: RaftMessage.AppendEntriesResult =>
        handleAppendEntriesResult(aer)

      case is: RaftMessage.InstallSnapshot =>
        handleInstallSnapshot(is)

      case isr: RaftMessage.InstallSnapshotResult =>
        handleInstallSnapshotResult(isr)

      case cr: RaftMessage.ClientRequest =>
        handleClientRequest(cr)

      case cq: RaftMessage.ClientQuery =>
        handleClientQuery(cq)

    // ---- Election ----

    private def handleElectionTimeout(): Unit =
      if state.role == Role.Leader then return

      val newTerm = state.term + 1
      context.log.info(s"[$nodeId] Election timeout - becoming Candidate for term $newTerm")

      state = state.copy(
        role = Role.Candidate,
        persistent = state.persistent.copy(currentTerm = newTerm, votedFor = Some(nodeId)),
        votesReceived = Set(nodeId),
        currentLeader = None
      )
      resetElectionTimer()

      // Request votes from all peers
      peers.foreach { (peerId, ref) =>
        ref ! RaftMessage.RequestVote(
          term = newTerm,
          candidateId = nodeId,
          lastLogIndex = state.lastLogIndex,
          lastLogTerm = state.lastLogTerm,
          replyTo = context.self
        )
      }

    private def handleRequestVote(rv: RaftMessage.RequestVote): Unit =
      // If the requester's term is higher, step down
      if rv.term > state.term then stepDown(rv.term)

      val logOk =
        rv.lastLogTerm > state.lastLogTerm ||
          (rv.lastLogTerm == state.lastLogTerm && rv.lastLogIndex >= state.lastLogIndex)

      val canVote =
        rv.term == state.term &&
          state.persistent.votedFor.forall(_ == rv.candidateId) &&
          logOk

      if canVote then
        state = state.copy(
          persistent = state.persistent.copy(votedFor = Some(rv.candidateId))
        )
        resetElectionTimer()
        context.log.debug(s"[$nodeId] Voting for ${rv.candidateId} in term ${rv.term}")

      rv.replyTo ! RaftMessage.RequestVoteResult(
        term = state.term,
        voteGranted = canVote,
        voterId = nodeId
      )

    private def handleRequestVoteResult(rvr: RaftMessage.RequestVoteResult): Unit =
      if rvr.term > state.term then
        stepDown(rvr.term)
        return

      if state.role != Role.Candidate || rvr.term != state.term then return

      if rvr.voteGranted then
        state = state.copy(votesReceived = state.votesReceived + rvr.voterId)
        val majority = (peers.size + 1) / 2 + 1
        if state.votesReceived.size >= majority then becomeLeader()

    private def becomeLeader(): Unit =
      context.log.info(s"[$nodeId] Became Leader for term ${state.term}")
      val nextIdx = state.lastLogIndex + 1
      val leaderState = LeaderVolatileState(
        nextIndex = peers.map((id, _) => id -> nextIdx),
        matchIndex = peers.map((id, _) => id -> 0)
      )
      state = state.copy(
        role = Role.Leader,
        leaderState = Some(leaderState),
        currentLeader = Some(nodeId),
        votesReceived = Set.empty
      )

      // Append a no-op entry to commit entries from prior terms
      appendToLog(Command.Noop)

      stopHeartbeatTimer()
      startHeartbeatTimer()
      startCompactionTimer()
      sendHeartbeats()

    // ---- AppendEntries (log replication & heartbeat) ----

    private def handleAppendEntries(ae: RaftMessage.AppendEntries): Unit =
      if ae.term < state.term then
        ae.replyTo ! RaftMessage.AppendEntriesResult(state.term, success = false, nodeId, 0)
        return

      if ae.term > state.term || state.role == Role.Candidate then
        stepDown(ae.term)

      state = state.copy(currentLeader = Some(ae.leaderId))
      resetElectionTimer()

      // Check log consistency
      val snapshotOffset = state.snapshot.map(_.lastIncludedIndex).getOrElse(0)

      val prevOk =
        ae.prevLogIndex == 0 ||
          ae.prevLogIndex == snapshotOffset && state.snapshot.exists(_.lastIncludedTerm == ae.prevLogTerm) ||
          (ae.prevLogIndex > snapshotOffset && state.termAt(ae.prevLogIndex) == ae.prevLogTerm)

      if !prevOk then
        ae.replyTo ! RaftMessage.AppendEntriesResult(state.term, success = false, nodeId, 0)
        return

      // Append entries
      if ae.entries.nonEmpty then
        val newLog = LogOps.appendEntries(state.persistent.log, ae.prevLogIndex, ae.entries, snapshotOffset)
        state = state.copy(
          persistent = state.persistent.copy(log = newLog)
        )

      // Update commit index
      if ae.leaderCommit > state.volatile.commitIndex then
        val newCommit = math.min(ae.leaderCommit, state.lastLogIndex)
        state = state.copy(
          volatile = state.volatile.copy(commitIndex = newCommit)
        )
        applyEntries()

      ae.replyTo ! RaftMessage.AppendEntriesResult(state.term, success = true, nodeId, state.lastLogIndex)

    private def handleAppendEntriesResult(aer: RaftMessage.AppendEntriesResult): Unit =
      if aer.term > state.term then
        stepDown(aer.term)
        return

      if state.role != Role.Leader || aer.term != state.term then return

      state.leaderState.foreach { ls =>
        if aer.success then
          val newMatchIndex = ls.matchIndex.updated(aer.followerId, aer.matchIndex)
          val newNextIndex = ls.nextIndex.updated(aer.followerId, aer.matchIndex + 1)
          state = state.copy(
            leaderState = Some(ls.copy(nextIndex = newNextIndex, matchIndex = newMatchIndex))
          )
          advanceCommitIndex()
        else
          // Decrement nextIndex and retry
          val prevNext = ls.nextIndex.getOrElse(aer.followerId, state.lastLogIndex + 1)
          val snapshotOffset = state.snapshot.map(_.lastIncludedIndex).getOrElse(0)
          val newNext = math.max(prevNext - 1, snapshotOffset + 1)

          if newNext <= snapshotOffset then
            // Follower is too far behind; send snapshot
            state.snapshot.foreach { snap =>
              peers.get(aer.followerId).foreach { ref =>
                ref ! RaftMessage.InstallSnapshot(state.term, nodeId, snap, context.self)
              }
            }
          else
            state = state.copy(
              leaderState = Some(ls.copy(nextIndex = ls.nextIndex.updated(aer.followerId, newNext)))
            )
            // Resend
            sendAppendEntries(aer.followerId)
      }

    private def sendHeartbeats(): Unit =
      peers.keys.foreach(sendAppendEntries)

    private def sendAppendEntries(peerId: String): Unit =
      peers.get(peerId).foreach { ref =>
        state.leaderState.foreach { ls =>
          val nextIdx = ls.nextIndex.getOrElse(peerId, 1)
          val snapshotOffset = state.snapshot.map(_.lastIncludedIndex).getOrElse(0)
          val prevLogIndex = nextIdx - 1
          val prevLogTerm = state.termAt(prevLogIndex)

          val localStart = nextIdx - snapshotOffset - 1
          val entries =
            if localStart >= 0 && localStart < state.persistent.log.size then
              state.persistent.log.slice(localStart, state.persistent.log.size)
            else
              Vector.empty

          ref ! RaftMessage.AppendEntries(
            term = state.term,
            leaderId = nodeId,
            prevLogIndex = prevLogIndex,
            prevLogTerm = prevLogTerm,
            entries = entries,
            leaderCommit = state.volatile.commitIndex,
            replyTo = context.self
          )
        }
      }

    private def advanceCommitIndex(): Unit =
      state.leaderState.foreach { ls =>
        val allMatchIndices = ls.matchIndex.values.toVector :+ state.lastLogIndex
        val sorted = allMatchIndices.sorted
        val majority = (peers.size + 1) / 2 + 1
        // The highest index replicated on a majority
        val newCommit = sorted(sorted.size - majority)

        if newCommit > state.volatile.commitIndex && state.termAt(newCommit) == state.term then
          state = state.copy(
            volatile = state.volatile.copy(commitIndex = newCommit)
          )
          applyEntries()
          respondToPendingClients()
      }

    // ---- Snapshot / log compaction ----

    private def handleInstallSnapshot(is: RaftMessage.InstallSnapshot): Unit =
      if is.term < state.term then
        is.replyTo ! RaftMessage.InstallSnapshotResult(state.term, nodeId)
        return

      if is.term > state.term then stepDown(is.term)

      state = state.copy(currentLeader = Some(is.leaderId))
      resetElectionTimer()

      // Apply the snapshot
      val snap = is.snapshot
      val remainingLog = state.persistent.log.dropWhile(_.index <= snap.lastIncludedIndex)
      state = state.copy(
        persistent = state.persistent.copy(log = remainingLog),
        volatile = state.volatile.copy(
          commitIndex = math.max(state.volatile.commitIndex, snap.lastIncludedIndex),
          lastApplied = snap.lastIncludedIndex
        ),
        snapshot = Some(snap),
        stateMachine = snap.data
      )

      context.log.info(s"[$nodeId] Installed snapshot up to index ${snap.lastIncludedIndex}")
      is.replyTo ! RaftMessage.InstallSnapshotResult(state.term, nodeId)

    private def handleInstallSnapshotResult(isr: RaftMessage.InstallSnapshotResult): Unit =
      if isr.term > state.term then
        stepDown(isr.term)
        return

      if state.role != Role.Leader then return

      // Update nextIndex/matchIndex for the follower
      state.snapshot.foreach { snap =>
        state.leaderState.foreach { ls =>
          state = state.copy(
            leaderState = Some(ls.copy(
              nextIndex = ls.nextIndex.updated(isr.followerId, snap.lastIncludedIndex + 1),
              matchIndex = ls.matchIndex.updated(isr.followerId, snap.lastIncludedIndex)
            ))
          )
        }
      }

    private def maybeCompact(): Unit =
      if state.persistent.log.size > CompactionThreshold then
        val compactUpTo = state.volatile.commitIndex
        if compactUpTo <= 0 then return

        val termAtCompact = state.termAt(compactUpTo)
        val snapshotOffset = state.snapshot.map(_.lastIncludedIndex).getOrElse(0)

        val snap = Snapshot(compactUpTo, termAtCompact, state.stateMachine)
        val remaining = LogOps.compact(state.persistent.log, compactUpTo, snapshotOffset)
        state = state.copy(
          persistent = state.persistent.copy(log = remaining),
          snapshot = Some(snap)
        )
        context.log.info(s"[$nodeId] Compacted log up to index $compactUpTo, ${remaining.size} entries remain")

    // ---- Client requests ----

    private def handleClientRequest(cr: RaftMessage.ClientRequest): Unit =
      if state.role != Role.Leader then
        cr.replyTo ! RaftMessage.ClientRedirect(state.currentLeader)
        return

      val entry = appendToLog(cr.command)
      pendingRequests = pendingRequests.updated(entry.index, cr.replyTo)
      sendHeartbeats()

    private def handleClientQuery(cq: RaftMessage.ClientQuery): Unit =
      if state.role != Role.Leader then
        cq.replyTo ! RaftMessage.ClientRedirect(state.currentLeader)
        return

      val value = state.stateMachine.get(cq.key)
      cq.replyTo ! RaftMessage.ClientOk(value)

    // ---- Helpers ----

    private def stepDown(newTerm: Int): Unit =
      context.log.info(s"[$nodeId] Stepping down to Follower for term $newTerm (was ${state.role} in term ${state.term})")
      state = state.copy(
        role = Role.Follower,
        persistent = state.persistent.copy(currentTerm = newTerm, votedFor = None),
        leaderState = None,
        votesReceived = Set.empty
      )
      stopHeartbeatTimer()
      stopCompactionTimer()
      resetElectionTimer()

    private def appendToLog(command: Command): LogEntry =
      val entry = LogEntry(
        term = state.term,
        index = state.lastLogIndex + 1,
        command = command
      )
      state = state.copy(
        persistent = state.persistent.copy(log = state.persistent.log :+ entry)
      )
      entry

    private def applyEntries(): Unit =
      val snapshotOffset = state.snapshot.map(_.lastIncludedIndex).getOrElse(0)
      val (newSm, newApplied) = LogOps.applyCommitted(
        state.stateMachine, state.persistent.log,
        state.volatile.lastApplied, state.volatile.commitIndex,
        snapshotOffset
      )
      state = state.copy(
        stateMachine = newSm,
        volatile = state.volatile.copy(lastApplied = newApplied)
      )

    private def respondToPendingClients(): Unit =
      val committed = state.volatile.commitIndex
      val toRespond = pendingRequests.filter((idx, _) => idx <= committed)
      toRespond.foreach { (idx, replyTo) =>
        // For Put commands, return the value that was set
        state.logEntry(idx) match
          case Some(LogEntry(_, _, Command.Put(k, v))) =>
            replyTo ! RaftMessage.ClientOk(Some(v))
          case _ =>
            replyTo ! RaftMessage.ClientOk(None)
      }
      pendingRequests = pendingRequests.removedAll(toRespond.keys)
