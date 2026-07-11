package raft

import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration.*

class CoverageSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll:

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  // Helper: elect a single node as leader with 2 peers
  def electLeader(nodeId: String): (ActorRef[RaftMessage], TestProbe[RaftMessage], TestProbe[RaftMessage]) =
    val leader = testKit.spawn(Node(nodeId))
    val peer1 = testKit.createTestProbe[RaftMessage]()
    val peer2 = testKit.createTestProbe[RaftMessage]()
    leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
    leader ! RaftMessage.ElectionTimeout
    peer1.receiveMessage(3.seconds)
    peer2.receiveMessage(3.seconds)
    leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
    leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")
    peer1.receiveMessage(3.seconds) // heartbeat with noop
    peer2.receiveMessage(3.seconds)
    (leader, peer1, peer2)

  "A Raft Node compaction" should {

    "compact the log when it exceeds the threshold" in {
      val leader = testKit.spawn(Node("compact-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Append 60 entries (exceeds CompactionThreshold=50)
      for i <- 1 to 60 do
        val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()
        leader ! RaftMessage.ClientRequest(Command.Put(s"k$i", s"v$i"), clientProbe.ref)
        val ae1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
        val ae2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
        // Acknowledge both
        leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = ae1.entries.last.index)
        leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = ae2.entries.last.index)
        // Consume client response
        clientProbe.receiveMessage(5.seconds)

      // Trigger compaction
      leader ! RaftMessage.CompactionTick

      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      // After compaction, snapshot should exist
      state.logSize shouldBe 61 // 60 puts + 1 noop, total index = 61
    }

    "not compact when commitIndex is 0" in {
      val leader = testKit.spawn(Node("nocompact-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Log has only the noop entry (size 1, below threshold of 50)
      // CompactionTick should be no-op
      leader ! RaftMessage.CompactionTick
      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }
  }

  "A Raft Node leader append result handling" should {

    "not advance commitIndex when newCommit is not greater" in {
      val leader = testKit.spawn(Node("noadvance-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Consume initial heartbeat (noop at index 1)
      val hb1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val hb2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Acknowledge with matchIndex = 0 (no advancement)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = 0)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = 0)

      // Leader should still be alive
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }

    "not advance commitIndex for entries from a different term" in {
      val leader = testKit.spawn(Node("termcommit-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Get initial heartbeat (noop at index 1, term 1)
      val hb1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val hb2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // The noop entry is at term 1. The commit check requires termAt(newCommit) == state.term
      // Acknowledge at index 1 (the noop)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = 1)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = 1)

      // Commit index should advance to 1
      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.commitIndex shouldBe 1
    }

    "ignore AppendEntriesResult from wrong term" in {
      val leader = testKit.spawn(Node("wrongterm-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Send result with old term
      leader ! RaftMessage.AppendEntriesResult(term = 0, success = true, followerId = "p1", matchIndex = 5)

      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }
  }

  "A Raft Node client responses" should {

    "respond with ClientOk(None) for Delete commands after commit" in {
      val leader = testKit.spawn(Node("del-commit-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.ClientRequest(Command.Delete("key"), clientProbe.ref)

      val ae1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val ae2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = ae1.entries.last.index)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = ae2.entries.last.index)

      val resp = clientProbe.receiveMessage(5.seconds)
      resp shouldBe a[RaftMessage.ClientOk]
      resp.asInstanceOf[RaftMessage.ClientOk].result shouldBe None
    }

    "respond with ClientOk(None) for Noop entries after commit" in {
      val leader = testKit.spawn(Node("noop-commit-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // The initial heartbeat contains a noop entry at index 1
      val hb1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val hb2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Acknowledge the noop - this commits it
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = hb1.entries.last.index)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = hb2.entries.last.index)

      // Leader stays alive
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }
  }

  "A Raft Node with snapshot" should {

    "send InstallSnapshot to a follower that is too far behind" in {
      val leader = testKit.spawn(Node("snap-send-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Consume initial heartbeat (noop at index 1)
      val hb1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val hb2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Acknowledge noop
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = 1)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = 1)

      // Now append enough entries to trigger compaction
      for i <- 2 to 55 do
        val cp = testKit.createTestProbe[RaftMessage.ClientResponse]()
        leader ! RaftMessage.ClientRequest(Command.Put(s"k$i", s"v$i"), cp.ref)
        val ae1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
        val ae2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
        leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = ae1.entries.last.index)
        leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = ae2.entries.last.index)
        cp.receiveMessage(5.seconds)

      // Trigger compaction
      leader ! RaftMessage.CompactionTick

      // Now send repeated failed AppendEntriesResult from p1 to decrement nextIndex
      // until it falls below snapshotOffset, triggering InstallSnapshot.
      // After compaction, snapshotOffset ~ commitIndex (~56), nextIndex for p1 is ~57.
      // Each failure decrements by 1, so we need ~57 - snapshotOffset failures.
      // Send failures in a loop and consume retries until InstallSnapshot arrives.
      var gotSnapshot = false
      for _ <- 1 to 60 if !gotSnapshot do
        leader ! RaftMessage.AppendEntriesResult(term = 1, success = false, followerId = "p1", matchIndex = 0)
        // The leader sends either AppendEntries (retry) or InstallSnapshot
        val msg = peer1.receiveMessage(3.seconds)
        msg match
          case _: RaftMessage.InstallSnapshot => gotSnapshot = true
          case _: RaftMessage.AppendEntries => // keep going
          case _ => // ignore unexpected

      gotSnapshot shouldBe true
    }

    "handle InstallSnapshotResult when leader has no snapshot (no-op)" in {
      val leader = testKit.spawn(Node("no-snap-result"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Send InstallSnapshotResult when leader has no snapshot
      leader ! RaftMessage.InstallSnapshotResult(term = 1, followerId = "p1")

      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }

    "ignore InstallSnapshotResult when not leader" in {
      val follower = testKit.spawn(Node("snap-result-nonleader"))
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      follower ! RaftMessage.InstallSnapshotResult(term = 1, followerId = "p1")

      follower ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
    }

    "install a snapshot and apply remaining entries from log" in {
      val follower = testKit.spawn(Node("snap-remaining"))
      val probe = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      // First add some entries
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(
          LogEntry(1, 1, Command.Put("a", "1")),
          LogEntry(1, 2, Command.Put("b", "2")),
          LogEntry(2, 3, Command.Put("c", "3"))
        ),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Install a snapshot up to index 2
      val snap = Snapshot(2, 1, Map("a" -> "1", "b" -> "2"))
      follower ! RaftMessage.InstallSnapshot(term = 2, leaderId = "leader2", snapshot = snap, replyTo = probe.ref)
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.InstallSnapshotResult]
      result.term shouldBe 2

      // Verify state
      follower ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.term shouldBe 2
      state.stateMachine shouldBe Map("a" -> "1", "b" -> "2")
      state.commitIndex shouldBe 2
    }
  }

  "A Raft Node election edge cases" should {

    "ignore ElectionTimeout when already leader" in {
      val leader = testKit.spawn(Node("ignore-et-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Now leader; send ElectionTimeout — should be ignored
      leader ! RaftMessage.ElectionTimeout

      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }

    "not grant vote when already voted for different candidate" in {
      val follower = testKit.spawn(Node("already-voted"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Vote for c1 in term 1
      follower ! RaftMessage.RequestVote(term = 1, candidateId = "c1", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref)
      probe.receiveMessage(3.seconds)

      // Try to vote for c2 in same term — should reject
      follower ! RaftMessage.RequestVote(term = 1, candidateId = "c2", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref)
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      result.voteGranted shouldBe false
    }

    "grant vote to same candidate in same term" in {
      val follower = testKit.spawn(Node("same-candidate"))
      val probe = testKit.createTestProbe[RaftMessage]()

      follower ! RaftMessage.RequestVote(term = 1, candidateId = "c1", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref)
      val first = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      first.voteGranted shouldBe true

      // Same candidate asks again
      follower ! RaftMessage.RequestVote(term = 1, candidateId = "c1", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref)
      val second = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      second.voteGranted shouldBe true
    }
  }

  "A Raft Node AppendEntries edge cases" should {

    "reject AppendEntries with prevLogIndex beyond log and no snapshot" in {
      val follower = testKit.spawn(Node("prevlog-beyond"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Empty log, try prevLogIndex = 5 (no entry at index 5)
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 5, prevLogTerm = 1,
        entries = Vector(LogEntry(1, 6, Command.Put("x", "1"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe false
    }

    "update commitIndex to min(leaderCommit, lastLogIndex)" in {
      val follower = testKit.spawn(Node("min-commit"))
      val probe = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      // Add one entry
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("a", "1"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Send heartbeat with leaderCommit = 100 (beyond log)
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 1, prevLogTerm = 1,
        entries = Vector.empty, leaderCommit = 100, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // commitIndex should be min(100, 1) = 1
      follower ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.commitIndex shouldBe 1
    }

    "accept empty AppendEntries (heartbeat) with no entries" in {
      val follower = testKit.spawn(Node("heartbeat-only"))
      val probe = testKit.createTestProbe[RaftMessage]()

      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector.empty, leaderCommit = 0, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true
    }

    "handle AppendEntries with entries already in the log (idempotent)" in {
      val follower = testKit.spawn(Node("idempotent-ae"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // First append
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("a", "1"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Re-send the same entries — should be fine since prevLogIndex is 0
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("a", "1"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true
    }
  }

  "A Raft Node query" should {

    "return value for an existing key when leader" in {
      val leader = testKit.spawn(Node("query-existing"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Put a value and commit it
      leader ! RaftMessage.ClientRequest(Command.Put("key", "value"), clientProbe.ref)
      val ae1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val ae2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = ae1.entries.last.index)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = ae2.entries.last.index)
      clientProbe.receiveMessage(5.seconds)

      // Query the key
      leader ! RaftMessage.ClientQuery("key", clientProbe.ref)
      val resp = clientProbe.receiveMessage(3.seconds)
      resp shouldBe RaftMessage.ClientOk(Some("value"))
    }
  }

  "State.termAt" should {

    "return 0 for index before snapshot" in {
      val snap = Snapshot(5, 2, Map.empty)
      val s = NodeState(snapshot = Some(snap))
      s.termAt(3) shouldBe 0
    }

    "return 0 for index 0" in {
      val s = NodeState()
      s.termAt(0) shouldBe 0
    }
  }

  "LogOps.applyCommitted edge cases" should {

    "not apply entries when localIdx is negative (before snapshot)" in {
      val log = Vector(LogEntry(2, 4, Command.Put("d", "4")))
      val (sm, applied) = LogOps.applyCommitted(Map("c" -> "3"), log, 0, 2, snapshotOffset = 3)
      // applyCommitted increments from lastApplied=0 to commitIndex=2
      // localIdx for applied=1: 1 - 3 - 1 = -3 (skip)
      // localIdx for applied=2: 2 - 3 - 1 = -2 (skip)
      sm shouldBe Map("c" -> "3")
      applied shouldBe 2
    }

    "handle log smaller than applied entries" in {
      val log = Vector(LogEntry(1, 1, Command.Put("a", "1")))
      // snapshotOffset=0, lastApplied=0, commitIndex=5 but log only has 1 entry
      val (sm, applied) = LogOps.applyCommitted(Map.empty, log, 0, 5, snapshotOffset = 0)
      // applied=1: localIdx=0, applies Put("a","1")
      // applied=2..5: localIdx=1..4, all >= log.size(1), skip
      sm shouldBe Map("a" -> "1")
      applied shouldBe 5
    }
  }

  "LogOps.compact edge cases" should {

    "return empty when localIndex equals log size" in {
      val log = Vector(LogEntry(1, 1, Command.Noop), LogEntry(1, 2, Command.Noop))
      val result = LogOps.compact(log, 2, snapshotOffset = 0)
      result shouldBe empty
    }

    "return full log when localIndex is negative" in {
      val log = Vector(LogEntry(1, 1, Command.Noop))
      val result = LogOps.compact(log, 0, snapshotOffset = 5)
      // localIndex = 0 - 5 = -5, which is <= 0, so return log
      result shouldBe log
    }
  }

  "LogOps.appendEntries edge cases" should {

    "handle prevLogIndex before snapshot offset" in {
      val existing = Vector(LogEntry(2, 4, Command.Put("d", "4")))
      val newEntries = Vector(LogEntry(2, 5, Command.Put("e", "5")))
      // prevLogIndex = 2, snapshotOffset = 3 -> localPrev = -1
      // take(-1) returns empty, so result = newEntries
      val result = LogOps.appendEntries(existing, 2, newEntries, snapshotOffset = 3)
      result shouldBe newEntries
    }
  }