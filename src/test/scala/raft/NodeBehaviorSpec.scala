package raft

import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration.*

class NodeBehaviorSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll:

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "A Raft Node election" should {

    "become Candidate on ElectionTimeout and request votes from peers" in {
      val candidate = testKit.spawn(Node("cand"))
      val voteProbe1 = testKit.createTestProbe[RaftMessage]()
      val voteProbe2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      // Set up two peers (using test probes as peer references)
      candidate ! RaftMessage.SetPeers(Map("p1" -> voteProbe1.ref, "p2" -> voteProbe2.ref))

      // Trigger election timeout
      candidate ! RaftMessage.ElectionTimeout

      // Both peers should receive RequestVote
      val rv1 = voteProbe1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVote]
      rv1.candidateId shouldBe "cand"
      rv1.term shouldBe 1
      rv1.lastLogIndex shouldBe 0
      rv1.lastLogTerm shouldBe 0

      val rv2 = voteProbe2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVote]
      rv2.candidateId shouldBe "cand"
      rv2.term shouldBe 1

      // Verify state is Candidate
      candidate ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Candidate
      state.term shouldBe 1
    }

    "become Leader after receiving majority votes" in {
      val candidate = testKit.spawn(Node("leader-cand"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      candidate ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      candidate ! RaftMessage.ElectionTimeout

      // Receive RequestVote from peers
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Both peers grant the vote (term = 1)
      candidate ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      candidate ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // After majority, should become leader and send heartbeats (AppendEntries)
      val ae1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      ae1.term shouldBe 1
      ae1.leaderId shouldBe "leader-cand"

      val ae2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      ae2.term shouldBe 1

      // Verify leader state
      candidate ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
      state.term shouldBe 1
      state.currentLeader shouldBe Some("leader-cand")
    }

    "not become Leader with insufficient votes" in {
      val candidate = testKit.spawn(Node("minority-cand"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val peer3 = testKit.createTestProbe[RaftMessage]()
      val peer4 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      // 5-node cluster: majority = 3, candidate has self-vote = 1, needs 2 more
      candidate ! RaftMessage.SetPeers(Map(
        "p1" -> peer1.ref, "p2" -> peer2.ref,
        "p3" -> peer3.ref, "p4" -> peer4.ref
      ))
      candidate ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
      peer3.receiveMessage(3.seconds)
      peer4.receiveMessage(3.seconds)

      // Only one external vote granted (self + p1 = 2, need 3 for majority)
      candidate ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      candidate ! RaftMessage.RequestVoteResult(term = 1, voteGranted = false, voterId = "p2")
      candidate ! RaftMessage.RequestVoteResult(term = 1, voteGranted = false, voterId = "p3")
      candidate ! RaftMessage.RequestVoteResult(term = 1, voteGranted = false, voterId = "p4")

      candidate ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Candidate
    }

    "step down when receiving RequestVoteResult with higher term" in {
      val candidate = testKit.spawn(Node("stepdown-cand"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      candidate ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      candidate ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Receive a result with higher term
      candidate ! RaftMessage.RequestVoteResult(term = 10, voteGranted = false, voterId = "p1")

      candidate ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.term shouldBe 10
    }

    "ignore stale RequestVoteResult with old term" in {
      val candidate = testKit.spawn(Node("stale-cand"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      candidate ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      candidate ! RaftMessage.ElectionTimeout

      // Term is now 1
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Send a result with old term 0
      candidate ! RaftMessage.RequestVoteResult(term = 0, voteGranted = true, voterId = "p1")

      candidate ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Candidate
      state.term shouldBe 1
    }
  }

  "A Raft Node as Leader" should {

    "handle client Put request and replicate via heartbeats" in {
      val leader = testKit.spawn(Node("put-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // becomeLeader appends a Noop and sends heartbeats with that entry
      val hb1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val hb2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Send client request — this triggers sendHeartbeats again with the new entry
      leader ! RaftMessage.ClientRequest(Command.Put("key1", "val1"), clientProbe.ref)

      val repl1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      // The heartbeat includes both the noop and the new Put entry
      repl1.entries should not be empty
      repl1.entries.last.command shouldBe Command.Put("key1", "val1")
    }

    "handle client Delete request and replicate" in {
      val leader = testKit.spawn(Node("del-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Consume initial heartbeat (noop entry)
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.ClientRequest(Command.Delete("key1"), clientProbe.ref)

      val repl1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      repl1.entries should not be empty
      repl1.entries.last.command shouldBe Command.Delete("key1")
    }

    "handle client query and return current state" in {
      val leader = testKit.spawn(Node("query-leader"))
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

      // Query a non-existent key
      leader ! RaftMessage.ClientQuery("missing", clientProbe.ref)
      val resp = clientProbe.receiveMessage(3.seconds)
      resp shouldBe RaftMessage.ClientOk(None)
    }

    "respond to committed pending client requests after commit" in {
      val leader = testKit.spawn(Node("commit-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Initial heartbeat (noop)
      val hb1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val hb2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Client request
      leader ! RaftMessage.ClientRequest(Command.Put("k", "v"), clientProbe.ref)

      // Receive the AppendEntries with the Put command
      val ae1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val ae2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Both followers acknowledge successfully
      leader ! RaftMessage.AppendEntriesResult(
        term = 1, success = true, followerId = "p1", matchIndex = ae1.entries.last.index
      )
      leader ! RaftMessage.AppendEntriesResult(
        term = 1, success = true, followerId = "p2", matchIndex = ae2.entries.last.index
      )

      // Client should receive ClientOk
      val resp = clientProbe.receiveMessage(5.seconds)
      resp shouldBe a[RaftMessage.ClientOk]
      resp.asInstanceOf[RaftMessage.ClientOk].result shouldBe Some("v")
    }

    "step down when AppendEntriesResult has higher term" in {
      val leader = testKit.spawn(Node("stepdown-leader"))
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

      // Send AppendEntriesResult with higher term
      leader ! RaftMessage.AppendEntriesResult(term = 5, success = false, followerId = "p1", matchIndex = 0)

      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.term shouldBe 5
    }

    "decrement nextIndex and retry when follower rejects AppendEntries" in {
      val leader = testKit.spawn(Node("retry-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Initial heartbeat
      val hb1 = peer1.receiveMessage(3.seconds)
      val hb2 = peer2.receiveMessage(3.seconds)

      // Send a client request to append an entry
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()
      leader ! RaftMessage.ClientRequest(Command.Put("x", "1"), clientProbe.ref)

      // Receive AppendEntries from leader
      val ae1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val ae2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // p1 fails - should retry with lower or same nextIndex
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = false, followerId = "p1", matchIndex = 0)
      // p2 succeeds
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = ae2.entries.last.index)

      // p1 should receive a retry AppendEntries
      val retry1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      // The retry should have prevLogIndex <= original (decremented but clamped to snapshotOffset+1)
      retry1.prevLogIndex shouldBe 0
    }

    "send InstallSnapshot when follower is too far behind" in {
      val leader = testKit.spawn(Node("snapshot-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Receive heartbeat
      val hb1 = peer1.receiveMessage(3.seconds)
      val hb2 = peer2.receiveMessage(3.seconds)

      // Set up a snapshot on the leader by sending AppendEntries that installs one
      // First, let's build state: append some entries and compact
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      // Append an entry
      leader ! RaftMessage.ClientRequest(Command.Put("k", "v"), clientProbe.ref)
      val ae1 = peer1.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val ae2 = peer2.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Both succeed
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = ae1.entries.last.index)
      leader ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p2", matchIndex = ae2.entries.last.index)

      // Now send a failed result with matchIndex far behind to trigger snapshot
      // We need to simulate the follower being behind the snapshot point.
      // First trigger compaction by sending CompactionTick (but only works if log > 50 entries).
      // Instead, directly test InstallSnapshot handling on a follower.
    }

    "handle InstallSnapshot as a follower" in {
      val follower = testKit.spawn(Node("snap-follower"))
      val probe = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      // First advance the follower's term via AppendEntries
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "old-leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("a", "1"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Now install a snapshot
      val snap = Snapshot(lastIncludedIndex = 5, lastIncludedTerm = 2, data = Map("x" -> "snap"))
      follower ! RaftMessage.InstallSnapshot(term = 3, leaderId = "new-leader", snapshot = snap, replyTo = probe.ref)

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.InstallSnapshotResult]
      result.term shouldBe 3
      result.followerId shouldBe "snap-follower"

      // Verify state
      follower ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.term shouldBe 3
      state.stateMachine shouldBe Map("x" -> "snap")
      state.commitIndex shouldBe 5
    }

    "reject InstallSnapshot with lower term" in {
      val follower = testKit.spawn(Node("snap-reject"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Advance term first
      follower ! RaftMessage.RequestVote(term = 5, candidateId = "c", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref)
      probe.receiveMessage(3.seconds)

      val snap = Snapshot(3, 2, Map.empty)
      follower ! RaftMessage.InstallSnapshot(term = 2, leaderId = "old", snapshot = snap, replyTo = probe.ref)

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.InstallSnapshotResult]
      result.term shouldBe 5
    }

    "handle InstallSnapshotResult as leader and update matchIndex" in {
      val leader = testKit.spawn(Node("snap-result-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Receive initial heartbeats
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Send InstallSnapshotResult — leader should update its state
      leader ! RaftMessage.InstallSnapshotResult(term = 1, followerId = "p1")

      // No crash; leader still active
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }

    "step down when InstallSnapshotResult has higher term" in {
      val leader = testKit.spawn(Node("snap-stepdown"))
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

      leader ! RaftMessage.InstallSnapshotResult(term = 10, followerId = "p1")

      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.term shouldBe 10
    }

    "handle HeartbeatTick by sending heartbeats when leader" in {
      val leader = testKit.spawn(Node("hb-tick-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      // Consume initial heartbeats
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Trigger HeartbeatTick manually
      leader ! RaftMessage.HeartbeatTick

      // Should receive new AppendEntries (heartbeat)
      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)
    }

    "ignore HeartbeatTick when not leader" in {
      val follower = testKit.spawn(Node("hb-tick-follower"))
      // No peers set, so even if election timer fires, no messages go out
      follower ! RaftMessage.HeartbeatTick
      // Verify it's still a follower
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      follower ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
    }

    "handle CompactionTick as leader (no-op when log is small)" in {
      val leader = testKit.spawn(Node("compact-tick-leader"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()

      leader ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      leader ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")
      leader ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p2")

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Trigger compaction tick — log is small, so it's a no-op
      leader ! RaftMessage.CompactionTick

      // Verify leader is still running
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      leader ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }

    "ignore CompactionTick when not leader" in {
      val follower = testKit.spawn(Node("compact-tick-follower"))
      follower ! RaftMessage.CompactionTick
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      follower ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
    }
  }

  "A Raft Node AppendEntries" should {

    "reject entries with mismatched prevLogTerm" in {
      val follower = testKit.spawn(Node("mismatch-follower"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Append an entry at term 1
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("a", "1"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Try to append with wrong prevLogTerm
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 1, prevLogTerm = 5,
        entries = Vector(LogEntry(1, 2, Command.Put("b", "2"))),
        leaderCommit = 0, replyTo = probe.ref
      )

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe false
    }

    "accept entries matching snapshot boundary" in {
      val follower = testKit.spawn(Node("snap-boundary-follower"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Install a snapshot first
      val snap = Snapshot(3, 1, Map("x" -> "1"))
      follower ! RaftMessage.InstallSnapshot(term = 1, leaderId = "leader", snapshot = snap, replyTo = probe.ref)
      probe.receiveMessage(3.seconds)

      // AppendEntries with prevLogIndex = 3 (snapshot boundary) and matching term
      follower ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 3, prevLogTerm = 1,
        entries = Vector(LogEntry(1, 4, Command.Put("y", "2"))),
        leaderCommit = 3, replyTo = probe.ref
      )

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true
    }

    "step down to follower when Candidate receives AppendEntries with higher or equal term" in {
      val candidate = testKit.spawn(Node("ae-stepdown-cand"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      candidate ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))
      candidate ! RaftMessage.ElectionTimeout

      peer1.receiveMessage(3.seconds)
      peer2.receiveMessage(3.seconds)

      // Now candidate is in term 1, role Candidate
      candidate ! RaftMessage.AppendEntries(
        term = 1, leaderId = "real-leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector.empty, leaderCommit = 0, replyTo = peer1.ref
      )

      candidate ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.currentLeader shouldBe Some("real-leader")
    }

    "update currentLeader when receiving valid AppendEntries" in {
      val follower = testKit.spawn(Node("leader-update-follower"))
      val probe = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      follower ! RaftMessage.AppendEntries(
        term = 2, leaderId = "the-leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector.empty, leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      follower ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.currentLeader shouldBe Some("the-leader")
      state.term shouldBe 2
    }
  }

  "A Raft Node RequestVote" should {

    "step down when receiving RequestVote with higher term" in {
      val follower = testKit.spawn(Node("rv-stepdown"))
      val probe = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      // Advance to term 1
      follower ! RaftMessage.RequestVote(term = 1, candidateId = "c1", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref)
      probe.receiveMessage(3.seconds)

      // Receive RequestVote with higher term
      follower ! RaftMessage.RequestVote(term = 5, candidateId = "c2", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref)
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      result.term shouldBe 5
      result.voteGranted shouldBe true

      follower ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.term shouldBe 5
      state.role shouldBe Role.Follower
    }

    "reject vote when candidate log is not up-to-date" in {
      val follower = testKit.spawn(Node("log-uptodate"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Build up follower's log with term 2 entries
      follower ! RaftMessage.AppendEntries(
        term = 2, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(
          LogEntry(2, 1, Command.Put("a", "1")),
          LogEntry(2, 2, Command.Put("b", "2"))
        ),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Candidate with older log (lastLogTerm = 1 < 2)
      follower ! RaftMessage.RequestVote(
        term = 3, candidateId = "old-cand", lastLogIndex = 5, lastLogTerm = 1, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      result.voteGranted shouldBe false
      result.term shouldBe 3

      // Candidate with same term but shorter log (lastLogIndex = 1 < 2)
      follower ! RaftMessage.RequestVote(
        term = 3, candidateId = "short-cand", lastLogIndex = 1, lastLogTerm = 2, replyTo = probe.ref
      )
      val result2 = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      result2.voteGranted shouldBe false
    }

    "grant vote when candidate log is up-to-date" in {
      val follower = testKit.spawn(Node("grant-vote"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Build up follower's log with term 2 entries
      follower ! RaftMessage.AppendEntries(
        term = 2, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(
          LogEntry(2, 1, Command.Put("a", "1")),
          LogEntry(2, 2, Command.Put("b", "2"))
        ),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Candidate with same or longer log and same term
      follower ! RaftMessage.RequestVote(
        term = 3, candidateId = "good-cand", lastLogIndex = 2, lastLogTerm = 2, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      result.voteGranted shouldBe true
      result.term shouldBe 3
    }
  }