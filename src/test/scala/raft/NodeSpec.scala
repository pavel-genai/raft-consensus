package raft

import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration.*

class NodeSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll:

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  /** Drain all pending messages from a probe within the given duration. */
  private def drainMessages[T](probe: TestProbe[T], duration: FiniteDuration = 500.millis): Unit =
    try
      while true do probe.receiveMessage(duration)
    catch
      case _: Throwable => () // no more messages

  "A Raft Node" should {

    "start as a Follower" in {
      val node = testKit.spawn(Node("test-node"))
      val probe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      node ! RaftMessage.GetState(probe.ref)
      val response = probe.receiveMessage(3.seconds)
      response.role shouldBe Role.Follower
      response.term shouldBe 0
      response.logSize shouldBe 0
      response.stateMachine shouldBe empty
    }

    "grant vote to a candidate with up-to-date log" in {
      val node = testKit.spawn(Node("voter"))
      val probe = testKit.createTestProbe[RaftMessage]()

      node ! RaftMessage.RequestVote(
        term = 1,
        candidateId = "candidate-1",
        lastLogIndex = 0,
        lastLogTerm = 0,
        replyTo = probe.ref
      )

      val result = probe.receiveMessage(3.seconds)
      result shouldBe a[RaftMessage.RequestVoteResult]
      val voteResult = result.asInstanceOf[RaftMessage.RequestVoteResult]
      voteResult.voteGranted shouldBe true
      voteResult.term shouldBe 1
    }

    "reject vote for a lower term" in {
      val node = testKit.spawn(Node("voter2"))
      val probe = testKit.createTestProbe[RaftMessage]()

      node ! RaftMessage.RequestVote(
        term = 5, candidateId = "c1", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      node ! RaftMessage.RequestVote(
        term = 3, candidateId = "c2", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      result.voteGranted shouldBe false
      result.term shouldBe 5
    }

    "not vote twice in the same term" in {
      val node = testKit.spawn(Node("voter3"))
      val probe = testKit.createTestProbe[RaftMessage]()

      node ! RaftMessage.RequestVote(
        term = 1, candidateId = "c1", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      val first = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      first.voteGranted shouldBe true

      node ! RaftMessage.RequestVote(
        term = 1, candidateId = "c2", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      val second = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      second.voteGranted shouldBe false
    }

    "accept AppendEntries from a valid leader" in {
      val node = testKit.spawn(Node("follower1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      node ! RaftMessage.AppendEntries(
        term = 1,
        leaderId = "leader-1",
        prevLogIndex = 0,
        prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("x", "1"))),
        leaderCommit = 0,
        replyTo = probe.ref
      )

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true
      result.term shouldBe 1
    }

    "reject AppendEntries with a lower term" in {
      val node = testKit.spawn(Node("follower2"))
      val probe = testKit.createTestProbe[RaftMessage]()

      node ! RaftMessage.RequestVote(
        term = 5, candidateId = "c", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      node ! RaftMessage.AppendEntries(
        term = 3, leaderId = "old-leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector.empty, leaderCommit = 0, replyTo = probe.ref
      )

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe false
      result.term shouldBe 5
    }

    "redirect client requests when not leader" in {
      val node = testKit.spawn(Node("follower3"))
      val probe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      node ! RaftMessage.ClientRequest(Command.Put("a", "1"), probe.ref)

      val response = probe.receiveMessage(3.seconds)
      response shouldBe a[RaftMessage.ClientRedirect]
    }

    "redirect client queries when not leader" in {
      val node = testKit.spawn(Node("follower4"))
      val probe = testKit.createTestProbe[RaftMessage.ClientResponse]()

      node ! RaftMessage.ClientQuery("key", probe.ref)

      val response = probe.receiveMessage(3.seconds)
      response shouldBe a[RaftMessage.ClientRedirect]
    }

    "update state after accepting AppendEntries with commit advance" in {
      val node = testKit.spawn(Node("follower5"))
      val aeProbe = testKit.createTestProbe[RaftMessage]()
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()

      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(
          LogEntry(1, 1, Command.Put("key", "value"))
        ),
        leaderCommit = 1,
        replyTo = aeProbe.ref
      )

      val result = aeProbe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true

      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.commitIndex shouldBe 1
      state.logSize shouldBe 1
      state.stateMachine shouldBe Map("key" -> "value")
    }

    "accept SetPeers and use them during election" in {
      val node = testKit.spawn(Node("setpeers-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()

      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // After election timeout the node should send RequestVote to those peers
      val rv1 = peer1.receiveMessage(2.seconds)
      rv1 shouldBe a[RaftMessage.RequestVote]
      val rv2 = peer2.receiveMessage(2.seconds)
      rv2 shouldBe a[RaftMessage.RequestVote]
    }

    "become Candidate after election timeout" in {
      val node = testKit.spawn(Node("election-1"))
      val peer = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer.ref))

      // Wait for election timeout (300-600ms)
      val rv = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      rv.term shouldBe 1
      rv.candidateId shouldBe "election-1"

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Candidate
      state.term shouldBe 1
    }

    "become Leader after receiving majority votes" in {
      val node = testKit.spawn(Node("leader-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Wait for election timeout -> becomes candidate
      val rv1 = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds) // consume RequestVote

      // Grant one vote (node already voted for itself, so 2/3 = majority)
      node ! RaftMessage.RequestVoteResult(term = rv1.term, voteGranted = true, voterId = "p1")

      // Leader sends heartbeats (AppendEntries) to peers
      val hb1 = peer1.receiveMessage(2.seconds)
      hb1 shouldBe a[RaftMessage.AppendEntries]
      val hb2 = peer2.receiveMessage(2.seconds)
      hb2 shouldBe a[RaftMessage.AppendEntries]

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
      state.term shouldBe rv1.term
      state.currentLeader shouldBe Some("leader-1")
    }

    "send periodic heartbeats as leader" in {
      val node = testKit.spawn(Node("heartbeat-1"))
      val peer = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer.ref))

      // Become candidate then leader
      val rv = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      // First heartbeat after becoming leader (includes Noop entry)
      val hb1 = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      hb1.leaderId shouldBe "heartbeat-1"

      // Should get another heartbeat after HeartbeatInterval (~150ms)
      val hb2 = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      hb2.leaderId shouldBe "heartbeat-1"
    }

    "leader advances commitIndex after majority replication" in {
      val node = testKit.spawn(Node("commit-adv-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv1 = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv1.term, voteGranted = true, voterId = "p1")

      // Drain initial heartbeats (Noop entry)
      val ae1 = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val ae2 = peer2.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Both followers acknowledge the Noop
      node ! RaftMessage.AppendEntriesResult(term = rv1.term, success = true, followerId = "p1", matchIndex = ae1.entries.last.index)
      node ! RaftMessage.AppendEntriesResult(term = rv1.term, success = true, followerId = "p2", matchIndex = ae2.entries.last.index)

      // Drain any heartbeats triggered by the result handling
      Thread.sleep(300)
      drainMessages(peer1)
      drainMessages(peer2)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.commitIndex should be >= 1
    }

    "leader decrements nextIndex on failed AppendEntriesResult and retries" in {
      val node = testKit.spawn(Node("retry-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      // Drain initial heartbeat
      peer1.receiveMessage(2.seconds)
      peer2.receiveMessage(2.seconds)

      // Report failure from p1
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = false, followerId = "p1", matchIndex = 0)

      // Leader should retry with a new AppendEntries to p1
      val retry = peer1.receiveMessage(2.seconds)
      retry shouldBe a[RaftMessage.AppendEntries]
    }

    "leader handles client request and responds after commit" in {
      val node = testKit.spawn(Node("client-req-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      // Drain initial heartbeats (Noop)
      val noopAe1 = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val noopAe2 = peer2.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]

      // Acknowledge Noop so commitIndex advances
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p1", matchIndex = noopAe1.entries.last.index)
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p2", matchIndex = noopAe2.entries.last.index)

      // Drain heartbeats triggered by acknowledgments
      Thread.sleep(200)
      drainMessages(peer1)
      drainMessages(peer2)

      // Send a client Put request
      val clientProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()
      node ! RaftMessage.ClientRequest(Command.Put("mykey", "myval"), clientProbe.ref)

      // Leader sends AppendEntries with the new entry to peers
      val putAe1 = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val putAe2 = peer2.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      putAe1.entries should not be empty

      // Acknowledge from majority
      val putMatchIndex = putAe1.entries.last.index
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p1", matchIndex = putMatchIndex)
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p2", matchIndex = putMatchIndex)

      // Client should get a response
      val resp = clientProbe.receiveMessage(3.seconds)
      resp shouldBe a[RaftMessage.ClientOk]
      resp.asInstanceOf[RaftMessage.ClientOk].result shouldBe Some("myval")
    }

    "leader handles client query and returns value from state machine" in {
      val node = testKit.spawn(Node("query-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      // Drain initial heartbeats
      peer1.receiveMessage(2.seconds)
      peer2.receiveMessage(2.seconds)

      // First put a value through AppendEntries + commit so state machine has data
      val aeProbe = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.ClientRequest(Command.Put("qkey", "qval"), testKit.createTestProbe[RaftMessage.ClientResponse]().ref)
      val ae1 = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val ae2 = peer2.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p1", matchIndex = ae1.entries.last.index)
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p2", matchIndex = ae2.entries.last.index)

      Thread.sleep(200)
      drainMessages(peer1)
      drainMessages(peer2)

      // Now query
      val queryProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()
      node ! RaftMessage.ClientQuery("qkey", queryProbe.ref)
      val resp = queryProbe.receiveMessage(3.seconds)
      resp shouldBe a[RaftMessage.ClientOk]
      resp.asInstanceOf[RaftMessage.ClientOk].result shouldBe Some("qval")
    }

    "leader returns None for unknown key in client query" in {
      val node = testKit.spawn(Node("query-none-1"))
      val peer = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer.ref))

      // Become leader (2 nodes = need 2 votes, self + p1)
      val rv = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")
      peer.receiveMessage(2.seconds) // drain heartbeat

      val queryProbe = testKit.createTestProbe[RaftMessage.ClientResponse]()
      node ! RaftMessage.ClientQuery("nonexistent", queryProbe.ref)
      val resp = queryProbe.receiveMessage(3.seconds)
      resp shouldBe RaftMessage.ClientOk(None)
    }

    "candidate steps down when receiving AppendEntries with higher term" in {
      val node = testKit.spawn(Node("stepdown-ae-1"))
      val peer = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer.ref))

      // Wait for election timeout -> becomes candidate at term 1
      val rv = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      rv.term shouldBe 1

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      stateProbe.receiveMessage(3.seconds).role shouldBe Role.Candidate

      // Send AppendEntries with a higher term
      val aeProbe = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.AppendEntries(
        term = 5, leaderId = "real-leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector.empty, leaderCommit = 0, replyTo = aeProbe.ref
      )
      val aeResult = aeProbe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      aeResult.success shouldBe true
      aeResult.term shouldBe 5

      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.term shouldBe 5
      state.currentLeader shouldBe Some("real-leader")
    }

    "candidate steps down when receiving RequestVoteResult with higher term" in {
      val node = testKit.spawn(Node("stepdown-rvr-1"))
      val peer = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer.ref))

      // becomes candidate
      val rv = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]

      // Send vote result with higher term
      node ! RaftMessage.RequestVoteResult(term = 10, voteGranted = false, voterId = "p1")

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.term shouldBe 10
    }

    "follower installs snapshot from leader" in {
      val node = testKit.spawn(Node("snap-install-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // First bump the term so snapshot with term=1 is accepted
      node ! RaftMessage.RequestVote(
        term = 1, candidateId = "c", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      val snap = Snapshot(5, 1, Map("a" -> "1", "b" -> "2"))
      node ! RaftMessage.InstallSnapshot(
        term = 1, leaderId = "snap-leader", snapshot = snap, replyTo = probe.ref
      )

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.InstallSnapshotResult]
      result.term shouldBe 1
      result.followerId shouldBe "snap-install-1"

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.stateMachine shouldBe Map("a" -> "1", "b" -> "2")
      state.commitIndex should be >= 5
      state.currentLeader shouldBe Some("snap-leader")
    }

    "reject InstallSnapshot with lower term" in {
      val node = testKit.spawn(Node("snap-reject-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Bump term to 5
      node ! RaftMessage.RequestVote(
        term = 5, candidateId = "c", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      val snap = Snapshot(3, 2, Map("x" -> "1"))
      node ! RaftMessage.InstallSnapshot(
        term = 2, leaderId = "old-leader", snapshot = snap, replyTo = probe.ref
      )

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.InstallSnapshotResult]
      result.term shouldBe 5

      // State machine should NOT have snapshot data
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.stateMachine shouldBe empty
    }

    "leader handles InstallSnapshotResult and updates indices" in {
      val node = testKit.spawn(Node("snap-result-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      // Drain heartbeats
      peer1.receiveMessage(2.seconds)
      peer2.receiveMessage(2.seconds)

      // Simulate receiving InstallSnapshotResult (from a hypothetical snapshot)
      // The leader should handle this without crashing
      node ! RaftMessage.InstallSnapshotResult(term = rv.term, followerId = "p1")

      // Node should still be leader
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }

    "reject AppendEntries with inconsistent prevLogIndex/prevLogTerm" in {
      val node = testKit.spawn(Node("inconsist-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // First append one entry at term 1
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("a", "1"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult].success shouldBe true

      // Now send entries with wrong prevLogTerm at prevLogIndex = 1
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 1, prevLogTerm = 99,
        entries = Vector(LogEntry(1, 2, Command.Put("b", "2"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe false
    }

    "reject AppendEntries with prevLogIndex beyond log end" in {
      val node = testKit.spawn(Node("beyond-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 5, prevLogTerm = 1,
        entries = Vector(LogEntry(1, 6, Command.Noop)),
        leaderCommit = 0, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe false
    }

    "append multiple entries in a single AppendEntries" in {
      val node = testKit.spawn(Node("multi-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      val entries = Vector(
        LogEntry(1, 1, Command.Put("a", "1")),
        LogEntry(1, 2, Command.Put("b", "2")),
        LogEntry(1, 3, Command.Put("c", "3"))
      )
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = entries, leaderCommit = 3, replyTo = probe.ref
      )

      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true
      result.matchIndex shouldBe 3

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.logSize shouldBe 3
      state.commitIndex shouldBe 3
      state.stateMachine shouldBe Map("a" -> "1", "b" -> "2", "c" -> "3")
    }

    "candidate steps down to follower when receiving AppendEntries from new leader" in {
      val node = testKit.spawn(Node("cand-stepdown-1"))
      val peer = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer.ref))

      // Wait to become candidate
      val rv = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]

      // New leader sends AppendEntries at the same term
      val aeProbe = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.AppendEntries(
        term = rv.term, leaderId = "new-leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector.empty, leaderCommit = 0, replyTo = aeProbe.ref
      )

      val result = aeProbe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.currentLeader shouldBe Some("new-leader")
    }

    "leader does not start new election on election timeout" in {
      val node = testKit.spawn(Node("leader-no-elect-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      // Drain heartbeats
      peer1.receiveMessage(2.seconds)
      peer2.receiveMessage(2.seconds)

      // Wait for what would be an election timeout period
      Thread.sleep(700)

      // Should still be leader with same term
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
      state.term shouldBe rv.term
    }

    "allow re-voting for the same candidate in the same term" in {
      val node = testKit.spawn(Node("revote-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // First vote
      node ! RaftMessage.RequestVote(
        term = 1, candidateId = "same-candidate", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      val first = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      first.voteGranted shouldBe true

      // Re-vote for same candidate in same term
      node ! RaftMessage.RequestVote(
        term = 1, candidateId = "same-candidate", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      val second = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      second.voteGranted shouldBe true
    }

    "leader steps down when receiving AppendEntriesResult with higher term" in {
      val node = testKit.spawn(Node("leader-stepdown-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")
      peer1.receiveMessage(2.seconds)
      peer2.receiveMessage(2.seconds)

      // AppendEntriesResult with higher term
      node ! RaftMessage.AppendEntriesResult(term = rv.term + 5, success = false, followerId = "p1", matchIndex = 0)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.term shouldBe rv.term + 5
    }

    "leader steps down when receiving AppendEntries with higher term" in {
      val node = testKit.spawn(Node("leader-stepdown-ae-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")
      peer1.receiveMessage(2.seconds)
      peer2.receiveMessage(2.seconds)

      // Higher-term AppendEntries from a different leader
      val aeProbe = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.AppendEntries(
        term = rv.term + 3, leaderId = "new-leader-2", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector.empty, leaderCommit = 0, replyTo = aeProbe.ref
      )
      aeProbe.receiveMessage(3.seconds)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.term shouldBe rv.term + 3
    }

    "reject vote for candidate with stale log (lower lastLogTerm)" in {
      val node = testKit.spawn(Node("stale-log-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Give the node a log entry at term 3
      node ! RaftMessage.AppendEntries(
        term = 3, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(3, 1, Command.Put("a", "1"))),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Candidate has lastLogTerm = 2 which is older
      node ! RaftMessage.RequestVote(
        term = 4, candidateId = "stale-candidate", lastLogIndex = 5, lastLogTerm = 2, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      result.voteGranted shouldBe false
    }

    "reject vote for candidate with shorter log (same term, lower index)" in {
      val node = testKit.spawn(Node("short-log-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Give the node 3 log entries at term 1
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(
          LogEntry(1, 1, Command.Put("a", "1")),
          LogEntry(1, 2, Command.Put("b", "2")),
          LogEntry(1, 3, Command.Put("c", "3"))
        ),
        leaderCommit = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Candidate has same term but shorter log
      node ! RaftMessage.RequestVote(
        term = 2, candidateId = "short-candidate", lastLogIndex = 1, lastLogTerm = 1, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.RequestVoteResult]
      result.voteGranted shouldBe false
    }

    "follower updates commitIndex to min(leaderCommit, lastLogIndex)" in {
      val node = testKit.spawn(Node("commit-min-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Append 2 entries
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(
          LogEntry(1, 1, Command.Put("a", "1")),
          LogEntry(1, 2, Command.Put("b", "2"))
        ),
        leaderCommit = 5, // leaderCommit > lastLogIndex
        replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      // commitIndex should be min(5, 2) = 2
      state.commitIndex shouldBe 2
    }

    "follower applies Delete commands through AppendEntries" in {
      val node = testKit.spawn(Node("delete-apply-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Put a key
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("x", "100"))),
        leaderCommit = 1, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Delete the key
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 1, prevLogTerm = 1,
        entries = Vector(LogEntry(1, 2, Command.Delete("x"))),
        leaderCommit = 2, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.stateMachine shouldBe empty
    }

    "follower handles heartbeat (empty AppendEntries) without changing log" in {
      val node = testKit.spawn(Node("heartbeat-empty-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // First add an entry
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Put("k", "v"))),
        leaderCommit = 1, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // Now send heartbeat (empty entries)
      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 1, prevLogTerm = 1,
        entries = Vector.empty, leaderCommit = 1, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.logSize shouldBe 1
      state.stateMachine shouldBe Map("k" -> "v")
    }

    "follower steps down from higher-term InstallSnapshot" in {
      val node = testKit.spawn(Node("snap-stepdown-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      // Node is at term 1
      node ! RaftMessage.RequestVote(
        term = 1, candidateId = "c", lastLogIndex = 0, lastLogTerm = 0, replyTo = probe.ref
      )
      probe.receiveMessage(3.seconds)

      // InstallSnapshot with higher term
      val snap = Snapshot(10, 3, Map("snapped" -> "data"))
      node ! RaftMessage.InstallSnapshot(
        term = 5, leaderId = "snap-leader-2", snapshot = snap, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.InstallSnapshotResult]
      result.term shouldBe 5

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.term shouldBe 5
      state.stateMachine shouldBe Map("snapped" -> "data")
    }

    "leader steps down on InstallSnapshotResult with higher term" in {
      val node = testKit.spawn(Node("snap-res-stepdown-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")
      peer1.receiveMessage(2.seconds)
      peer2.receiveMessage(2.seconds)

      // InstallSnapshotResult with higher term
      node ! RaftMessage.InstallSnapshotResult(term = rv.term + 10, followerId = "p1")

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
      state.term shouldBe rv.term + 10
    }

    "non-leader ignores InstallSnapshotResult" in {
      val node = testKit.spawn(Node("snap-res-ignore-1"))
      // Send InstallSnapshotResult to a follower; should not crash
      node ! RaftMessage.InstallSnapshotResult(term = 1, followerId = "p1")

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
    }

    "non-leader ignores AppendEntriesResult" in {
      val node = testKit.spawn(Node("ae-res-ignore-1"))
      // Send AppendEntriesResult to a follower; should not crash
      node ! RaftMessage.AppendEntriesResult(term = 1, success = true, followerId = "p1", matchIndex = 1)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
    }

    "non-candidate ignores RequestVoteResult" in {
      val node = testKit.spawn(Node("rvr-ignore-1"))
      // Send RequestVoteResult to a follower; should not crash
      node ! RaftMessage.RequestVoteResult(term = 1, voteGranted = true, voterId = "p1")

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
    }

    "candidate does not become leader without majority" in {
      val node = testKit.spawn(Node("no-majority-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val peer3 = testKit.createTestProbe[RaftMessage]()
      val peer4 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map(
        "p1" -> peer1.ref, "p2" -> peer2.ref,
        "p3" -> peer3.ref, "p4" -> peer4.ref
      ))

      // Wait for election
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      peer3.receiveMessage(2.seconds)
      peer4.receiveMessage(2.seconds)

      // Only one vote (self + p1 = 2/5 < majority of 3)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Candidate
    }

    "candidate becomes leader with exact majority in 5-node cluster" in {
      val node = testKit.spawn(Node("exact-majority-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      val peer3 = testKit.createTestProbe[RaftMessage]()
      val peer4 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map(
        "p1" -> peer1.ref, "p2" -> peer2.ref,
        "p3" -> peer3.ref, "p4" -> peer4.ref
      ))

      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      peer3.receiveMessage(2.seconds)
      peer4.receiveMessage(2.seconds)

      // Need 3 votes total (self + 2 peers) for majority of 5
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p2")

      // Drain heartbeats
      Thread.sleep(300)
      drainMessages(peer1)
      drainMessages(peer2)
      drainMessages(peer3)
      drainMessages(peer4)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Leader
    }

    "follower applies Noop through AppendEntries without changing state machine" in {
      val node = testKit.spawn(Node("noop-apply-1"))
      val probe = testKit.createTestProbe[RaftMessage]()

      node ! RaftMessage.AppendEntries(
        term = 1, leaderId = "leader", prevLogIndex = 0, prevLogTerm = 0,
        entries = Vector(LogEntry(1, 1, Command.Noop)),
        leaderCommit = 1, replyTo = probe.ref
      )
      val result = probe.receiveMessage(3.seconds).asInstanceOf[RaftMessage.AppendEntriesResult]
      result.success shouldBe true

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.stateMachine shouldBe empty
      state.commitIndex shouldBe 1
    }

    "candidate ignores vote for wrong term" in {
      val node = testKit.spawn(Node("wrong-term-vote-1"))
      val peer = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer.ref))

      // Becomes candidate at term 1
      val rv = peer.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      rv.term shouldBe 1

      // Send vote result for a different (old) term
      node ! RaftMessage.RequestVoteResult(term = 0, voteGranted = true, voterId = "p1")

      // Should still be candidate, not leader
      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      node ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Candidate
    }

    "leader sends Noop entry immediately after election" in {
      val node = testKit.spawn(Node("noop-leader-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      // First heartbeat should contain the Noop entry
      val hb = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      hb.entries should not be empty
      hb.entries.head.command shouldBe Command.Noop
    }

    "leader handles multiple client requests sequentially" in {
      val node = testKit.spawn(Node("multi-client-1"))
      val peer1 = testKit.createTestProbe[RaftMessage]()
      val peer2 = testKit.createTestProbe[RaftMessage]()
      node ! RaftMessage.SetPeers(Map("p1" -> peer1.ref, "p2" -> peer2.ref))

      // Become leader
      val rv = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.RequestVote]
      peer2.receiveMessage(2.seconds)
      node ! RaftMessage.RequestVoteResult(term = rv.term, voteGranted = true, voterId = "p1")

      // Drain Noop heartbeats
      val noop1 = peer1.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      val noop2 = peer2.receiveMessage(2.seconds).asInstanceOf[RaftMessage.AppendEntries]
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p1", matchIndex = noop1.entries.last.index)
      node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p2", matchIndex = noop2.entries.last.index)

      Thread.sleep(200)
      drainMessages(peer1)
      drainMessages(peer2)

      // Send two client requests
      val client1 = testKit.createTestProbe[RaftMessage.ClientResponse]()
      val client2 = testKit.createTestProbe[RaftMessage.ClientResponse]()
      node ! RaftMessage.ClientRequest(Command.Put("k1", "v1"), client1.ref)
      node ! RaftMessage.ClientRequest(Command.Put("k2", "v2"), client2.ref)

      // Gather and acknowledge - collect AppendEntries messages from peers
      Thread.sleep(200)
      var maxMatch1 = 0
      var maxMatch2 = 0
      try
        while true do
          val m = peer1.receiveMessage(500.millis)
          m match
            case ae: RaftMessage.AppendEntries =>
              ae.entries.foreach(e => maxMatch1 = math.max(maxMatch1, e.index))
            case _ =>
      catch case _: Throwable => ()
      try
        while true do
          val m = peer2.receiveMessage(500.millis)
          m match
            case ae: RaftMessage.AppendEntries =>
              ae.entries.foreach(e => maxMatch2 = math.max(maxMatch2, e.index))
            case _ =>
      catch case _: Throwable => ()

      if maxMatch1 > 0 then
        node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p1", matchIndex = maxMatch1)
      if maxMatch2 > 0 then
        node ! RaftMessage.AppendEntriesResult(term = rv.term, success = true, followerId = "p2", matchIndex = maxMatch2)

      // Both clients should eventually get responses
      val resp1 = client1.receiveMessage(3.seconds)
      resp1 shouldBe a[RaftMessage.ClientOk]
      val resp2 = client2.receiveMessage(3.seconds)
      resp2 shouldBe a[RaftMessage.ClientOk]
    }
  }
