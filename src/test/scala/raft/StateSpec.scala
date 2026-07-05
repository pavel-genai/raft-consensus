package raft

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class StateSpec extends AnyWordSpec with Matchers:

  "NodeState" should {

    "start with sensible defaults" in {
      val s = NodeState()
      s.role shouldBe Role.Follower
      s.term shouldBe 0
      s.log shouldBe empty
      s.lastLogIndex shouldBe 0
      s.lastLogTerm shouldBe 0
      s.stateMachine shouldBe empty
    }

    "compute lastLogIndex from log size" in {
      val entries = Vector(
        LogEntry(1, 1, Command.Noop),
        LogEntry(1, 2, Command.Put("a", "1")),
        LogEntry(2, 3, Command.Put("b", "2"))
      )
      val s = NodeState(persistent = PersistentState(currentTerm = 2, log = entries))
      s.lastLogIndex shouldBe 3
    }

    "compute lastLogIndex accounting for snapshot offset" in {
      val entries = Vector(LogEntry(3, 6, Command.Put("c", "3")))
      val snap = Snapshot(5, 2, Map.empty)
      val s = NodeState(
        persistent = PersistentState(log = entries),
        snapshot = Some(snap)
      )
      s.lastLogIndex shouldBe 6
    }

    "compute lastLogTerm from the last entry" in {
      val entries = Vector(
        LogEntry(1, 1, Command.Noop),
        LogEntry(3, 2, Command.Put("x", "y"))
      )
      val s = NodeState(persistent = PersistentState(log = entries))
      s.lastLogTerm shouldBe 3
    }

    "compute lastLogTerm from snapshot when log is empty" in {
      val snap = Snapshot(5, 2, Map.empty)
      val s = NodeState(snapshot = Some(snap))
      s.lastLogTerm shouldBe 2
    }

    "return 0 for lastLogTerm when both log and snapshot are empty" in {
      NodeState().lastLogTerm shouldBe 0
    }

    "retrieve a log entry by 1-based index" in {
      val entries = Vector(
        LogEntry(1, 1, Command.Put("a", "1")),
        LogEntry(1, 2, Command.Put("b", "2"))
      )
      val s = NodeState(persistent = PersistentState(log = entries))
      s.logEntry(1) shouldBe Some(entries(0))
      s.logEntry(2) shouldBe Some(entries(1))
      s.logEntry(3) shouldBe None
      s.logEntry(0) shouldBe None
    }

    "retrieve log entries with snapshot offset" in {
      val snap = Snapshot(3, 1, Map.empty)
      val entries = Vector(
        LogEntry(2, 4, Command.Put("d", "4")),
        LogEntry(2, 5, Command.Put("e", "5"))
      )
      val s = NodeState(
        persistent = PersistentState(log = entries),
        snapshot = Some(snap)
      )
      s.logEntry(4) shouldBe Some(entries(0))
      s.logEntry(5) shouldBe Some(entries(1))
      s.logEntry(3) shouldBe None
      s.logEntry(6) shouldBe None
    }

    "return correct term at a given index" in {
      val entries = Vector(
        LogEntry(1, 1, Command.Noop),
        LogEntry(2, 2, Command.Put("a", "1")),
        LogEntry(2, 3, Command.Put("b", "2"))
      )
      val s = NodeState(persistent = PersistentState(log = entries))
      s.termAt(0) shouldBe 0
      s.termAt(1) shouldBe 1
      s.termAt(2) shouldBe 2
      s.termAt(3) shouldBe 2
      s.termAt(4) shouldBe 0
    }

    "return snapshot term for the snapshot boundary index" in {
      val snap = Snapshot(3, 1, Map.empty)
      val entries = Vector(LogEntry(2, 4, Command.Noop))
      val s = NodeState(
        persistent = PersistentState(log = entries),
        snapshot = Some(snap)
      )
      s.termAt(3) shouldBe 1
      s.termAt(4) shouldBe 2
    }
  }

  "PersistentState" should {
    "default to term 0 with no vote and empty log" in {
      val ps = PersistentState()
      ps.currentTerm shouldBe 0
      ps.votedFor shouldBe None
      ps.log shouldBe empty
    }
  }

  "VolatileState" should {
    "default to zero indices" in {
      val vs = VolatileState()
      vs.commitIndex shouldBe 0
      vs.lastApplied shouldBe 0
    }
  }

  "LeaderVolatileState" should {
    "default to empty maps" in {
      val ls = LeaderVolatileState()
      ls.nextIndex shouldBe empty
      ls.matchIndex shouldBe empty
    }
  }

  "Role" should {
    "have three variants" in {
      Role.values should contain theSameElementsAs Seq(Role.Follower, Role.Candidate, Role.Leader)
    }
  }
