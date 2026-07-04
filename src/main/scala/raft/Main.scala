package raft

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.util.Timeout
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

/**
 * CLI entry point that spins up a 5-node Raft cluster in a single JVM
 * and lets the user send key-value commands to the leader.
 */
@main def run(): Unit =
  println("Starting Raft cluster with 5 nodes...")
  val system = Cluster.startSystem(5)
  given ActorSystem[ClusterCommand] = system
  given Timeout = Timeout(3.seconds)
  import system.executionContext

  // Wait for a leader to be elected
  println("Waiting for leader election...")
  Thread.sleep(2000)

  var running = true
  printHelp()

  while running do
    print("raft> ")
    val line = StdIn.readLine()
    if line == null then running = false
    else
      val parts = line.trim.split("\\s+").toList
      parts match
        case "put" :: key :: value :: Nil =>
          withLeader { (leaderId, leaderRef) =>
            val resp = Await.result(
              leaderRef.ask[RaftMessage.ClientResponse](r =>
                RaftMessage.ClientRequest(Command.Put(key, value), r)
              ),
              3.seconds
            )
            resp match
              case RaftMessage.ClientOk(_) =>
                println(s"  OK: $key = $value (via leader $leaderId)")
              case RaftMessage.ClientRedirect(leader) =>
                println(s"  Redirected to $leader")
              case RaftMessage.ClientError(msg) =>
                println(s"  Error: $msg")
          }

        case "get" :: key :: Nil =>
          withLeader { (leaderId, leaderRef) =>
            val resp = Await.result(
              leaderRef.ask[RaftMessage.ClientResponse](r =>
                RaftMessage.ClientQuery(key, r)
              ),
              3.seconds
            )
            resp match
              case RaftMessage.ClientOk(Some(v)) =>
                println(s"  $key = $v")
              case RaftMessage.ClientOk(None) =>
                println(s"  $key not found")
              case RaftMessage.ClientRedirect(leader) =>
                println(s"  Redirected to $leader")
              case RaftMessage.ClientError(msg) =>
                println(s"  Error: $msg")
          }

        case "delete" :: key :: Nil =>
          withLeader { (leaderId, leaderRef) =>
            val resp = Await.result(
              leaderRef.ask[RaftMessage.ClientResponse](r =>
                RaftMessage.ClientRequest(Command.Delete(key), r)
              ),
              3.seconds
            )
            resp match
              case RaftMessage.ClientOk(_) =>
                println(s"  OK: deleted $key (via leader $leaderId)")
              case other =>
                println(s"  $other")
          }

        case "status" :: Nil =>
          printClusterStatus()

        case "help" :: Nil =>
          printHelp()

        case "quit" :: Nil | "exit" :: Nil =>
          running = false

        case Nil | ("" :: Nil) => // ignore empty input

        case _ =>
          println(s"  Unknown command. Type 'help' for usage.")

  println("Shutting down cluster...")
  system.terminate()
  Await.result(system.whenTerminated, 5.seconds)
  println("Goodbye.")

  def withLeader(f: (String, ActorRef[RaftMessage]) => Unit)(
      using system: ActorSystem[ClusterCommand], timeout: Timeout
  ): Unit =
    import system.executionContext
    val nodesResp = Await.result(
      system.ask[NodesResponse](r => ClusterCommand.GetNodes(r)),
      3.seconds
    )
    Cluster.findLeader(nodesResp.nodes) match
      case Some((id, ref)) => Try(f(id, ref)) match
        case Failure(ex) => println(s"  Error: ${ex.getMessage}")
        case Success(_)  => // ok
      case None =>
        println("  No leader found. The cluster may still be electing.")

  def printClusterStatus()(
      using system: ActorSystem[ClusterCommand], timeout: Timeout
  ): Unit =
    import system.executionContext
    val nodesResp = Await.result(
      system.ask[NodesResponse](r => ClusterCommand.GetNodes(r)),
      3.seconds
    )
    nodesResp.nodes.foreach { (id, ref) =>
      Try {
        val resp = Await.result(
          ref.ask[RaftMessage.NodeStateResponse](r => RaftMessage.GetState(r)),
          2.seconds
        )
        println(f"  ${resp.nodeId}%-10s role=${resp.role}%-10s term=${resp.term}%-4d " +
          f"commit=${resp.commitIndex}%-4d log=${resp.logSize}%-4d leader=${resp.currentLeader.getOrElse("?")}")
      }.recover { case ex =>
        println(f"  $id%-10s UNREACHABLE")
      }
    }

  def printHelp(): Unit =
    println("""
      |Commands:
      |  put <key> <value>  - Store a key-value pair
      |  get <key>          - Retrieve a value by key
      |  delete <key>       - Delete a key
      |  status             - Show cluster status
      |  help               - Show this help
      |  quit               - Exit
      |""".stripMargin)
