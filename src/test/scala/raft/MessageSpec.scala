package raft

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class MessageSpec extends AnyWordSpec with Matchers:

  "RaftMessage.RequestVoteResult" should {
    "store term, voteGranted, and voterId" in {
      val rvr = RaftMessage.RequestVoteResult(term = 3, voteGranted = true, voterId = "node-2")
      rvr.term shouldBe 3
      rvr.voteGranted shouldBe true
      rvr.voterId shouldBe "node-2"
    }

    "distinguish granted from denied" in {
      val granted = RaftMessage.RequestVoteResult(1, voteGranted = true, "n1")
      val denied = RaftMessage.RequestVoteResult(1, voteGranted = false, "n1")
      granted.voteGranted shouldBe true
      denied.voteGranted shouldBe false
      granted should not be denied
    }
  }

  "RaftMessage.AppendEntriesResult" should {
    "store term, success, followerId, and matchIndex" in {
      val aer = RaftMessage.AppendEntriesResult(term = 5, success = true, followerId = "f1", matchIndex = 10)
      aer.term shouldBe 5
      aer.success shouldBe true
      aer.followerId shouldBe "f1"
      aer.matchIndex shouldBe 10
    }

    "distinguish success from failure" in {
      val ok = RaftMessage.AppendEntriesResult(2, success = true, "f1", 5)
      val fail = RaftMessage.AppendEntriesResult(2, success = false, "f1", 0)
      ok.success shouldBe true
      fail.success shouldBe false
    }
  }

  "RaftMessage.InstallSnapshotResult" should {
    "store term and followerId" in {
      val isr = RaftMessage.InstallSnapshotResult(term = 4, followerId = "f2")
      isr.term shouldBe 4
      isr.followerId shouldBe "f2"
    }
  }

  "RaftMessage.ClientResponse variants" should {
    "construct ClientOk with a result" in {
      val ok = RaftMessage.ClientOk(Some("value"))
      ok.result shouldBe Some("value")
    }

    "construct ClientOk with None (default)" in {
      val ok = RaftMessage.ClientOk()
      ok.result shouldBe None
    }

    "construct ClientOk with explicit None" in {
      val ok = RaftMessage.ClientOk(None)
      ok.result shouldBe None
    }

    "construct ClientRedirect with a leader" in {
      val redirect = RaftMessage.ClientRedirect(Some("leader-1"))
      redirect.leaderId shouldBe Some("leader-1")
    }

    "construct ClientRedirect with no leader" in {
      val redirect = RaftMessage.ClientRedirect(None)
      redirect.leaderId shouldBe None
    }

    "construct ClientError with a message" in {
      val err = RaftMessage.ClientError("something went wrong")
      err.message shouldBe "something went wrong"
    }

    "be distinguishable as sealed trait members" in {
      val ok: RaftMessage.ClientResponse = RaftMessage.ClientOk(Some("v"))
      val redirect: RaftMessage.ClientResponse = RaftMessage.ClientRedirect(Some("l"))
      val error: RaftMessage.ClientResponse = RaftMessage.ClientError("e")

      ok shouldBe a[RaftMessage.ClientOk]
      redirect shouldBe a[RaftMessage.ClientRedirect]
      error shouldBe a[RaftMessage.ClientError]
    }
  }

  "RaftMessage.NodeStateResponse" should {
    "store all fields" in {
      val resp = RaftMessage.NodeStateResponse(
        nodeId = "node-1",
        role = Role.Leader,
        term = 5,
        commitIndex = 10,
        logSize = 12,
        stateMachine = Map("x" -> "1", "y" -> "2"),
        currentLeader = Some("node-1")
      )
      resp.nodeId shouldBe "node-1"
      resp.role shouldBe Role.Leader
      resp.term shouldBe 5
      resp.commitIndex shouldBe 10
      resp.logSize shouldBe 12
      resp.stateMachine shouldBe Map("x" -> "1", "y" -> "2")
      resp.currentLeader shouldBe Some("node-1")
    }

    "handle follower state with no leader" in {
      val resp = RaftMessage.NodeStateResponse(
        nodeId = "node-3",
        role = Role.Follower,
        term = 0,
        commitIndex = 0,
        logSize = 0,
        stateMachine = Map.empty,
        currentLeader = None
      )
      resp.role shouldBe Role.Follower
      resp.currentLeader shouldBe None
      resp.stateMachine shouldBe empty
    }
  }

  "RaftMessage timer messages" should {
    "be singleton objects" in {
      RaftMessage.ElectionTimeout shouldBe RaftMessage.ElectionTimeout
      RaftMessage.HeartbeatTick shouldBe RaftMessage.HeartbeatTick
      RaftMessage.CompactionTick shouldBe RaftMessage.CompactionTick
    }

    "be instances of RaftMessage" in {
      (RaftMessage.ElectionTimeout: RaftMessage) shouldBe a[RaftMessage]
      (RaftMessage.HeartbeatTick: RaftMessage) shouldBe a[RaftMessage]
      (RaftMessage.CompactionTick: RaftMessage) shouldBe a[RaftMessage]
    }
  }

  "NodesResponse" should {
    "store a map of nodes" in {
      val resp = NodesResponse(Map.empty)
      resp.nodes shouldBe empty
    }
  }

  "ClusterCommand variants" should {
    "include Start" in {
      ClusterCommand.Start shouldBe ClusterCommand.Start
    }

    "include StopNode with nodeId" in {
      val cmd = ClusterCommand.StopNode("node-1")
      cmd.nodeId shouldBe "node-1"
    }

    "include RestartNode with nodeId" in {
      val cmd = ClusterCommand.RestartNode("node-3")
      cmd.nodeId shouldBe "node-3"
    }
  }
