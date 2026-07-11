package raft

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class CliSpec extends AnyWordSpec with Matchers:

  "Cli.parseCommand" should {

    "parse put command" in {
      Cli.parseCommand("put key value") shouldBe CliCommand.PutCmd("key", "value")
    }

    "parse get command" in {
      Cli.parseCommand("get key") shouldBe CliCommand.GetCmd("key")
    }

    "parse delete command" in {
      Cli.parseCommand("delete key") shouldBe CliCommand.DeleteCmd("key")
    }

    "parse status command" in {
      Cli.parseCommand("status") shouldBe CliCommand.StatusCmd
    }

    "parse help command" in {
      Cli.parseCommand("help") shouldBe CliCommand.HelpCmd
    }

    "parse quit command" in {
      Cli.parseCommand("quit") shouldBe CliCommand.QuitCmd
    }

    "parse exit command" in {
      Cli.parseCommand("exit") shouldBe CliCommand.QuitCmd
    }

    "parse empty line as noop" in {
      Cli.parseCommand("") shouldBe CliCommand.NoopCmd
    }

    "parse whitespace-only line as noop" in {
      Cli.parseCommand("   ") shouldBe CliCommand.NoopCmd
    }

    "parse unknown command" in {
      Cli.parseCommand("frobnicate") shouldBe CliCommand.UnknownCmd
    }

    "parse put with extra spaces" in {
      Cli.parseCommand("  put   key   value  ") shouldBe CliCommand.PutCmd("key", "value")
    }
  }

  "Cli.formatResponse" should {

    "format Put success" in {
      val result = Cli.formatResponse(CliCommand.PutCmd("k", "v"), RaftMessage.ClientOk(Some("v")), "leader-1")
      result shouldBe "  OK: k = v (via leader leader-1)"
    }

    "format Put redirect" in {
      val result = Cli.formatResponse(CliCommand.PutCmd("k", "v"), RaftMessage.ClientRedirect(Some("other")), "leader-1")
      result shouldBe "  Redirected to Some(other)"
    }

    "format Put error" in {
      val result = Cli.formatResponse(CliCommand.PutCmd("k", "v"), RaftMessage.ClientError("bad"), "leader-1")
      result shouldBe "  Error: bad"
    }

    "format Get with value" in {
      val result = Cli.formatResponse(CliCommand.GetCmd("k"), RaftMessage.ClientOk(Some("val")), "leader-1")
      result shouldBe "  k = val"
    }

    "format Get not found" in {
      val result = Cli.formatResponse(CliCommand.GetCmd("k"), RaftMessage.ClientOk(None), "leader-1")
      result shouldBe "  k not found"
    }

    "format Get redirect" in {
      val result = Cli.formatResponse(CliCommand.GetCmd("k"), RaftMessage.ClientRedirect(Some("other")), "leader-1")
      result shouldBe "  Redirected to Some(other)"
    }

    "format Get error" in {
      val result = Cli.formatResponse(CliCommand.GetCmd("k"), RaftMessage.ClientError("oops"), "leader-1")
      result shouldBe "  Error: oops"
    }

    "format Delete success" in {
      val result = Cli.formatResponse(CliCommand.DeleteCmd("k"), RaftMessage.ClientOk(None), "leader-1")
      result shouldBe "  OK: deleted k (via leader leader-1)"
    }

    "format Delete with redirect" in {
      val result = Cli.formatResponse(CliCommand.DeleteCmd("k"), RaftMessage.ClientRedirect(None), "leader-1")
      result should include("Redirect")
    }
  }

  "Cli.formatNodeState" should {

    "format a node state with all fields" in {
      val resp = RaftMessage.NodeStateResponse(
        nodeId = "node-1", role = Role.Leader, term = 3,
        commitIndex = 10, logSize = 15, stateMachine = Map.empty,
        currentLeader = Some("node-1")
      )
      val s = Cli.formatNodeState(resp)
      s should include("node-1")
      s should include("Leader")
      s should include("3")
      s should include("10")
      s should include("15")
      s should include("node-1")
    }

    "format a node state with no leader" in {
      val resp = RaftMessage.NodeStateResponse(
        nodeId = "node-2", role = Role.Follower, term = 1,
        commitIndex = 0, logSize = 0, stateMachine = Map.empty,
        currentLeader = None
      )
      val s = Cli.formatNodeState(resp)
      s should include("Follower")
      s should include("?")
    }
  }

  "Cli.formatUnreachable" should {

    "format an unreachable node" in {
      val s = Cli.formatUnreachable("dead-node")
      s should include("dead-node")
      s should include("UNREACHABLE")
    }
  }

  "Cli.helpText" should {

    "contain all command descriptions" in {
      Cli.helpText should include("put")
      Cli.helpText should include("get")
      Cli.helpText should include("delete")
      Cli.helpText should include("status")
      Cli.helpText should include("help")
      Cli.helpText should include("quit")
    }
  }

  "Cli.noLeaderMsg" should {

    "contain a helpful message" in {
      Cli.noLeaderMsg should include("No leader")
    }
  }

  "CliCommand" should {

    "have all expected variants" in {
      // Just verify the enum values exist and can be pattern matched
      val cmds = List(
        CliCommand.PutCmd("k", "v"),
        CliCommand.GetCmd("k"),
        CliCommand.DeleteCmd("k"),
        CliCommand.StatusCmd,
        CliCommand.HelpCmd,
        CliCommand.QuitCmd,
        CliCommand.NoopCmd,
        CliCommand.UnknownCmd
      )
      cmds.length shouldBe 8
    }
  }