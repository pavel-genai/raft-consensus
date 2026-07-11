package raft

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.util.Timeout
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

/**
 * CLI helper functions for the Raft cluster.
 * Extracted from Main to allow unit testing without stdin.
 */
object Cli:

  /** Parse a command line into a structured command. */
  def parseCommand(line: String): CliCommand =
    val parts = line.trim.split("\\s+").toList
    parts match
      case "put" :: key :: value :: Nil    => CliCommand.PutCmd(key, value)
      case "get" :: key :: Nil             => CliCommand.GetCmd(key)
      case "delete" :: key :: Nil          => CliCommand.DeleteCmd(key)
      case "status" :: Nil                 => CliCommand.StatusCmd
      case "help" :: Nil                   => CliCommand.HelpCmd
      case "quit" :: Nil | "exit" :: Nil   => CliCommand.QuitCmd
      case Nil | ("" :: Nil)               => CliCommand.NoopCmd
      case _                                => CliCommand.UnknownCmd

  /** Format a client response for display. */
  def formatResponse(cmd: CliCommand, resp: RaftMessage.ClientResponse, leaderId: String): String =
    (cmd, resp) match
      case (PutCmd(key, value), RaftMessage.ClientOk(_)) =>
        s"  OK: $key = $value (via leader $leaderId)"
      case (PutCmd(_, _), RaftMessage.ClientRedirect(leader)) =>
        s"  Redirected to $leader"
      case (PutCmd(_, _), RaftMessage.ClientError(msg)) =>
        s"  Error: $msg"
      case (GetCmd(key), RaftMessage.ClientOk(Some(v))) =>
        s"  $key = $v"
      case (GetCmd(key), RaftMessage.ClientOk(None)) =>
        s"  $key not found"
      case (GetCmd(_), RaftMessage.ClientRedirect(leader)) =>
        s"  Redirected to $leader"
      case (GetCmd(_), RaftMessage.ClientError(msg)) =>
        s"  Error: $msg"
      case (DeleteCmd(key), RaftMessage.ClientOk(_)) =>
        s"  OK: deleted $key (via leader $leaderId)"
      case (DeleteCmd(_), other) =>
        s"  $other"
      case _ => ""

  /** Format a node state for the status display. */
  def formatNodeState(resp: RaftMessage.NodeStateResponse): String =
    f"  ${resp.nodeId}%-10s role=${resp.role}%-10s term=${resp.term}%-4d " +
    f"commit=${resp.commitIndex}%-4d log=${resp.logSize}%-4d leader=${resp.currentLeader.getOrElse("?")}"

  /** Format an unreachable node. */
  def formatUnreachable(nodeId: String): String =
    f"  $nodeId%-10s UNREACHABLE"

  /** Help text. */
  val helpText: String =
    """
      |Commands:
      |  put <key> <value>  - Store a key-value pair
      |  get <key>          - Retrieve a value by key
      |  delete <key>       - Delete a key
      |  status             - Show cluster status
      |  help               - Show this help
      |  quit               - Exit
      |""".stripMargin

  /** No leader message. */
  val noLeaderMsg: String = "  No leader found. The cluster may still be electing."

/** Parsed CLI commands. */
enum CliCommand:
  case PutCmd(key: String, value: String)
  case GetCmd(key: String)
  case DeleteCmd(key: String)
  case StatusCmd
  case HelpCmd
  case QuitCmd
  case NoopCmd
  case UnknownCmd

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

  println("Waiting for leader election...")
  Thread.sleep(2000)

  var running = true
  print(Cli.helpText)

  while running do
    print("raft> ")
    val line = StdIn.readLine()
    if line == null then running = false
    else
      Cli.parseCommand(line) match
        case cmd: CliCommand.PutCmd =>
          withLeader { (leaderId, leaderRef) =>
            val resp = Await.result(
              leaderRef.ask[RaftMessage.ClientResponse](r =>
                RaftMessage.ClientRequest(Command.Put(cmd.key, cmd.value), r)
              ),
              3.seconds
            )
            println(Cli.formatResponse(cmd, resp, leaderId))
          }

        case cmd: CliCommand.GetCmd =>
          withLeader { (leaderId, leaderRef) =>
            val resp = Await.result(
              leaderRef.ask[RaftMessage.ClientResponse](r =>
                RaftMessage.ClientQuery(cmd.key, r)
              ),
              3.seconds
            )
            println(Cli.formatResponse(cmd, resp, leaderId))
          }

        case cmd: CliCommand.DeleteCmd =>
          withLeader { (leaderId, leaderRef) =>
            val resp = Await.result(
              leaderRef.ask[RaftMessage.ClientResponse](r =>
                RaftMessage.ClientRequest(Command.Delete(cmd.key), r)
              ),
              3.seconds
            )
            println(Cli.formatResponse(cmd, resp, leaderId))
          }

        case CliCommand.StatusCmd => printClusterStatus()
        case CliCommand.HelpCmd   => print(Cli.helpText)
        case CliCommand.QuitCmd   => running = false
        case CliCommand.NoopCmd   => ()
        case CliCommand.UnknownCmd => println("  Unknown command. Type 'help' for usage.")

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
        case Success(_)  => ()
      case None =>
        println(Cli.noLeaderMsg)

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
        println(Cli.formatNodeState(resp))
      }.recover { case _ =>
        println(Cli.formatUnreachable(id))
      }
    }