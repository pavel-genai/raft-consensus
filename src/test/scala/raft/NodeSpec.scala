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
  }
