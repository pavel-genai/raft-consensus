package raft

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.util.Timeout

/**
 * Messages understood by the cluster guardian actor.
 */
sealed trait ClusterCommand
object ClusterCommand:
  case object Start extends ClusterCommand
  case class GetNodes(replyTo: ActorRef[NodesResponse]) extends ClusterCommand
  case class StopNode(nodeId: String) extends ClusterCommand
  case class RestartNode(nodeId: String) extends ClusterCommand

final case class NodesResponse(nodes: Map[String, ActorRef[RaftMessage]])

/**
 * Manages a cluster of Raft nodes within a single JVM.
 *
 * Spawns N nodes, wires them together, and provides operations to
 * stop/restart individual nodes for fault-tolerance testing.
 */
object Cluster:

  def apply(nodeIds: Seq[String]): Behavior[ClusterCommand] =
    Behaviors.setup { context =>
      var nodes: Map[String, ActorRef[RaftMessage]] = Map.empty
      var stoppedNodes: Set[String] = Set.empty

      nodeIds.foreach { id =>
        val ref = context.spawn(Node(id), s"raft-node-$id")
        nodes = nodes.updated(id, ref)
      }

      // Wire peers
      nodes.foreach { (id, ref) =>
        val otherPeers = nodes.removed(id)
        ref ! RaftMessage.SetPeers(otherPeers)
      }

      Behaviors.receiveMessage {
        case ClusterCommand.Start =>
          context.log.info(s"Cluster started with nodes: ${nodeIds.mkString(", ")}")
          Behaviors.same

        case ClusterCommand.GetNodes(replyTo) =>
          replyTo ! NodesResponse(nodes)
          Behaviors.same

        case ClusterCommand.StopNode(nodeId) =>
          nodes.get(nodeId).foreach { ref =>
            context.stop(ref)
            stoppedNodes = stoppedNodes + nodeId
            // Tell remaining nodes to remove this peer
            nodes.removed(nodeId).foreach { (_, peerRef) =>
              val updatedPeers = nodes.removed(nodeId).removed(nodeId)
              // We don't update peers dynamically here in the simple version;
              // the stopped node simply becomes unresponsive.
            }
            context.log.info(s"Stopped node $nodeId")
          }
          Behaviors.same

        case ClusterCommand.RestartNode(nodeId) =>
          if stoppedNodes.contains(nodeId) then
            val ref = context.spawn(Node(nodeId), s"raft-node-$nodeId-${System.nanoTime()}")
            nodes = nodes.updated(nodeId, ref)
            stoppedNodes = stoppedNodes - nodeId

            // Re-wire all peers
            nodes.foreach { (id, r) =>
              r ! RaftMessage.SetPeers(nodes.removed(id))
            }
            context.log.info(s"Restarted node $nodeId")
          Behaviors.same
      }

  /**
   * Convenience: create and start a cluster ActorSystem.
   */
  def startSystem(nodeCount: Int = 5): ActorSystem[ClusterCommand] =
    val ids = (1 to nodeCount).map(i => s"node-$i")
    val system = ActorSystem(Cluster(ids), "raft-cluster")
    system ! ClusterCommand.Start
    system

  /**
   * Find the current leader by querying all nodes.
   */
  def findLeader(
      nodes: Map[String, ActorRef[RaftMessage]]
  )(using system: ActorSystem[?]): Option[(String, ActorRef[RaftMessage])] =
    given Timeout = Timeout(2.seconds)
    import system.executionContext

    val futures = nodes.map { (id, ref) =>
      ref.ask[RaftMessage.NodeStateResponse](r => RaftMessage.GetState(r))
        .map(resp => (id, ref, resp))
        .recover { case _ => (id, ref, null) }
    }
    val results = Await.result(Future.sequence(futures), 3.seconds)
    results
      .filter(_._3 != null)
      .find(_._3.role == Role.Leader)
      .map(r => (r._1, r._2))
