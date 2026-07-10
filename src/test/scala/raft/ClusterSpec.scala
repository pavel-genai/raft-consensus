package raft

import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration.*
import org.apache.pekko.util.Timeout

class ClusterSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll:

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  given Timeout = Timeout(3.seconds)

  "A Cluster" should {

    "spawn the requested number of nodes" in {
      val cluster = testKit.spawn(Cluster(Seq("n1", "n2", "n3")))
      val probe = testKit.createTestProbe[NodesResponse]()
      cluster ! ClusterCommand.GetNodes(probe.ref)
      val resp = probe.receiveMessage(3.seconds)
      resp.nodes.keySet shouldBe Set("n1", "n2", "n3")
    }

    "respond to Start" in {
      val cluster = testKit.spawn(Cluster(Seq("a", "b")))
      cluster ! ClusterCommand.Start
      // no crash — just stays same behavior
      val probe = testKit.createTestProbe[NodesResponse]()
      cluster ! ClusterCommand.GetNodes(probe.ref)
      probe.receiveMessage(3.seconds).nodes.keySet shouldBe Set("a", "b")
    }

    "stop a node and restart it" in {
      val cluster = testKit.spawn(Cluster(Seq("x", "y", "z")))
      val probe = testKit.createTestProbe[NodesResponse]()

      // Get initial nodes
      cluster ! ClusterCommand.GetNodes(probe.ref)
      val initial = probe.receiveMessage(3.seconds)
      initial.nodes.keySet shouldBe Set("x", "y", "z")

      // Stop node y
      cluster ! ClusterCommand.StopNode("y")
      // Allow time for the stop to take effect
      Thread.sleep(200)

      // Get nodes — y should still be in map but stopped
      cluster ! ClusterCommand.GetNodes(probe.ref)
      val afterStop = probe.receiveMessage(3.seconds)
      afterStop.nodes.keySet shouldBe Set("x", "y", "z")

      // Restart node y
      cluster ! ClusterCommand.RestartNode("y")
      Thread.sleep(200)

      cluster ! ClusterCommand.GetNodes(probe.ref)
      val afterRestart = probe.receiveMessage(3.seconds)
      afterRestart.nodes.keySet shouldBe Set("x", "y", "z")
    }

    "not restart a node that was not stopped" in {
      val cluster = testKit.spawn(Cluster(Seq("p", "q")))
      val probe = testKit.createTestProbe[NodesResponse]()

      // Restart without stopping first — should be a no-op
      cluster ! ClusterCommand.RestartNode("p")
      cluster ! ClusterCommand.GetNodes(probe.ref)
      val resp = probe.receiveMessage(3.seconds)
      resp.nodes.keySet shouldBe Set("p", "q")
    }

    "startSystem creates a working ActorSystem" in {
      val system = Cluster.startSystem(3)
      try
        import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
        import scala.concurrent.{Await, ExecutionContext}
        given ExecutionContext = system.executionContext
        given org.apache.pekko.actor.typed.ActorSystem[?] = system
        val nodesResp = Await.result(
          system.ask[NodesResponse](r => ClusterCommand.GetNodes(r)),
          3.seconds
        )
        nodesResp.nodes.keySet shouldBe Set("node-1", "node-2", "node-3")
      finally
        system.terminate()
    }
  }