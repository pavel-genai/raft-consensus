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

    "return 0 for termAt with index below snapshot" in {
      val snap = Snapshot(5, 3, Map("a" -> "1"))
      val entries = Vector(LogEntry(4, 6, Command.Put("b", "2")))
      val s = NodeState(
        persistent = PersistentState(log = entries),
        snapshot = Some(snap)
      )
      s.termAt(2) shouldBe 0
      s.termAt(4) shouldBe 0
    }

    "return None for logEntry at negative or zero index with no snapshot" in {
      val entries = Vector(LogEntry(1, 1, Command.Put("a", "1")))
      val s = NodeState(persistent = PersistentState(log = entries))
      s.logEntry(0) shouldBe None
      s.logEntry(-1) shouldBe None
      s.logEntry(-100) shouldBe None
    }

    "preserve all fields via copy" in {
      val ls = LeaderVolatileState(
        nextIndex = Map("n2" -> 5),
        matchIndex = Map("n2" -> 4)
      )
      val snap = Snapshot(3, 1, Map("x" -> "1"))
      val s = NodeState(
        role = Role.Leader,
        persistent = PersistentState(currentTerm = 5, votedFor = Some("n1"), log = Vector(LogEntry(5, 4, Command.Noop))),
        volatile = VolatileState(commitIndex = 4, lastApplied = 4),
        leaderState = Some(ls),
        snapshot = Some(snap),
        stateMachine = Map("x" -> "1", "y" -> "2"),
        votesReceived = Set("n1", "n2", "n3"),
        currentLeader = Some("n1")
      )
      val s2 = s.copy(role = Role.Follower)
      s2.role shouldBe Role.Follower
      s2.persistent shouldBe s.persistent
      s2.volatile shouldBe s.volatile
      s2.leaderState shouldBe s.leaderState
      s2.snapshot shouldBe s.snapshot
      s2.stateMachine shouldBe s.stateMachine
      s2.votesReceived shouldBe s.votesReceived
      s2.currentLeader shouldBe s.currentLeader
    }

    "track leaderState correctly" in {
      val ls = LeaderVolatileState(
        nextIndex = Map("n2" -> 3, "n3" -> 3),
        matchIndex = Map("n2" -> 2, "n3" -> 1)
      )
      val s = NodeState(role = Role.Leader, leaderState = Some(ls))
      s.leaderState shouldBe defined
      s.leaderState.get.nextIndex("n2") shouldBe 3
      s.leaderState.get.matchIndex("n3") shouldBe 1

      val s2 = NodeState()
      s2.leaderState shouldBe None
    }

    "track votesReceived" in {
      val s = NodeState(role = Role.Candidate, votesReceived = Set("n1"))
      s.votesReceived should contain("n1")
      val s2 = s.copy(votesReceived = s.votesReceived + "n2")
      s2.votesReceived should contain allOf("n1", "n2")
      s2.votesReceived.size shouldBe 2
    }

    "track currentLeader" in {
      val s = NodeState()
      s.currentLeader shouldBe None
      val s2 = s.copy(currentLeader = Some("leader-1"))
      s2.currentLeader shouldBe Some("leader-1")
    }

    "return 0 for lastLogIndex with empty log and no snapshot" in {
      val s = NodeState()
      s.lastLogIndex shouldBe 0
    }

    "return 0 for termAt beyond log length" in {
      val entries = Vector(
        LogEntry(1, 1, Command.Noop),
        LogEntry(2, 2, Command.Put("a", "1"))
      )
      val s = NodeState(persistent = PersistentState(log = entries))
      s.termAt(3) shouldBe 0
      s.termAt(100) shouldBe 0
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

  "Snapshot" should {
    "store lastIncludedIndex, lastIncludedTerm, and data" in {
      val data = Map("key1" -> "val1", "key2" -> "val2")
      val snap = Snapshot(10, 3, data)
      snap.lastIncludedIndex shouldBe 10
      snap.lastIncludedTerm shouldBe 3
      snap.data shouldBe data
    }

    "support empty data" in {
      val snap = Snapshot(0, 0, Map.empty)
      snap.data shouldBe empty
      snap.lastIncludedIndex shouldBe 0
      snap.lastIncludedTerm shouldBe 0
    }

    "support equality" in {
      val s1 = Snapshot(5, 2, Map("a" -> "1"))
      val s2 = Snapshot(5, 2, Map("a" -> "1"))
      val s3 = Snapshot(5, 3, Map("a" -> "1"))
      s1 shouldBe s2
      s1 should not be s3
    }
  }
