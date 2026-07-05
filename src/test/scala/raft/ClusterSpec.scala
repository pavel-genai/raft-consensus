package raft

import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration.*

class ClusterSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll:

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "Cluster" should {

    "spawn a cluster with given node IDs and return them via GetNodes" in {
      val ids = Seq("c1-a", "c1-b", "c1-c")
      val cluster = testKit.spawn(Cluster(ids))
      val probe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.GetNodes(probe.ref)
      val resp = probe.receiveMessage(3.seconds)
      resp.nodes.keySet shouldBe Set("c1-a", "c1-b", "c1-c")
      resp.nodes.size shouldBe 3
    }

    "handle Start command without error" in {
      val ids = Seq("c2-a", "c2-b")
      val cluster = testKit.spawn(Cluster(ids))
      val probe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.Start

      cluster ! ClusterCommand.GetNodes(probe.ref)
      val resp = probe.receiveMessage(3.seconds)
      resp.nodes.size shouldBe 2
    }

    "return correct node count for a single-node cluster" in {
      val ids = Seq("c3-solo")
      val cluster = testKit.spawn(Cluster(ids))
      val probe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.GetNodes(probe.ref)
      val resp = probe.receiveMessage(3.seconds)
      resp.nodes.size shouldBe 1
      resp.nodes.keySet shouldBe Set("c3-solo")
    }

    "return correct node count for a five-node cluster" in {
      val ids = (1 to 5).map(i => s"c4-n$i")
      val cluster = testKit.spawn(Cluster(ids))
      val probe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.GetNodes(probe.ref)
      val resp = probe.receiveMessage(3.seconds)
      resp.nodes.size shouldBe 5
      resp.nodes.keySet shouldBe (1 to 5).map(i => s"c4-n$i").toSet
    }

    "stop a node so it becomes unreachable" in {
      val ids = Seq("c5-a", "c5-b", "c5-c")
      val cluster = testKit.spawn(Cluster(ids))
      val nodesProbe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.GetNodes(nodesProbe.ref)
      val resp = nodesProbe.receiveMessage(3.seconds)
      val nodeRef = resp.nodes("c5-b")

      cluster ! ClusterCommand.StopNode("c5-b")
      Thread.sleep(500)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      nodeRef ! RaftMessage.GetState(stateProbe.ref)
      stateProbe.expectNoMessage(1.second)
    }

    "handle StopNode for a non-existent node ID gracefully" in {
      val ids = Seq("c6-a", "c6-b")
      val cluster = testKit.spawn(Cluster(ids))
      val probe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.StopNode("c6-nonexistent")

      cluster ! ClusterCommand.GetNodes(probe.ref)
      val resp = probe.receiveMessage(3.seconds)
      resp.nodes.size shouldBe 2
    }

    "restart a previously stopped node" in {
      val ids = Seq("c7-a", "c7-b", "c7-c")
      val cluster = testKit.spawn(Cluster(ids))
      val nodesProbe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.StopNode("c7-b")
      Thread.sleep(500)

      cluster ! ClusterCommand.RestartNode("c7-b")
      Thread.sleep(500)

      cluster ! ClusterCommand.GetNodes(nodesProbe.ref)
      val resp = nodesProbe.receiveMessage(3.seconds)
      resp.nodes.keySet should contain("c7-b")

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      resp.nodes("c7-b") ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.role shouldBe Role.Follower
    }

    "treat RestartNode as no-op for a node that was never stopped" in {
      val ids = Seq("c8-a", "c8-b")
      val cluster = testKit.spawn(Cluster(ids))
      val probe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.GetNodes(probe.ref)
      val before = probe.receiveMessage(3.seconds)
      val originalRef = before.nodes("c8-a")

      cluster ! ClusterCommand.RestartNode("c8-a")
      Thread.sleep(300)

      cluster ! ClusterCommand.GetNodes(probe.ref)
      val after = probe.receiveMessage(3.seconds)
      after.nodes("c8-a") shouldBe originalRef
    }

    "wire peers so nodes can communicate" in {
      val ids = Seq("c9-a", "c9-b", "c9-c")
      val cluster = testKit.spawn(Cluster(ids))
      val nodesProbe = testKit.createTestProbe[NodesResponse]()

      cluster ! ClusterCommand.GetNodes(nodesProbe.ref)
      val resp = nodesProbe.receiveMessage(3.seconds)

      val stateProbe = testKit.createTestProbe[RaftMessage.NodeStateResponse]()
      resp.nodes("c9-a") ! RaftMessage.GetState(stateProbe.ref)
      val state = stateProbe.receiveMessage(3.seconds)
      state.nodeId shouldBe "c9-a"
      state.term shouldBe 0
    }
  }
