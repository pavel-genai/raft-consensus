package raft

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class LogOpsSpec extends AnyWordSpec with Matchers:

  "LogOps.appendEntries" should {

    "return existing log when new entries are empty" in {
      val existing = Vector(LogEntry(1, 1, Command.Noop))
      LogOps.appendEntries(existing, 1, Vector.empty) shouldBe existing
    }

    "append entries to an empty log" in {
      val entries = Vector(
        LogEntry(1, 1, Command.Put("a", "1")),
        LogEntry(1, 2, Command.Put("b", "2"))
      )
      val result = LogOps.appendEntries(Vector.empty, 0, entries)
      result shouldBe entries
    }

    "append entries after existing ones" in {
      val existing = Vector(LogEntry(1, 1, Command.Put("a", "1")))
      val newEntries = Vector(LogEntry(1, 2, Command.Put("b", "2")))
      val result = LogOps.appendEntries(existing, 1, newEntries)
      result.size shouldBe 2
      result(1) shouldBe newEntries(0)
    }

    "truncate conflicting entries and append new ones" in {
      val existing = Vector(
        LogEntry(1, 1, Command.Put("a", "1")),
        LogEntry(1, 2, Command.Put("b", "2")),
        LogEntry(1, 3, Command.Put("c", "3"))
      )
      val newEntries = Vector(
        LogEntry(2, 2, Command.Put("b", "new")),
        LogEntry(2, 3, Command.Put("d", "4"))
      )
      val result = LogOps.appendEntries(existing, 1, newEntries)
      result.size shouldBe 3
      result(0) shouldBe existing(0)
      result(1).term shouldBe 2
      result(2).command shouldBe Command.Put("d", "4")
    }

    "handle snapshot offset" in {
      val existing = Vector(LogEntry(2, 4, Command.Put("d", "4")))
      val newEntries = Vector(LogEntry(2, 5, Command.Put("e", "5")))
      val result = LogOps.appendEntries(existing, 4, newEntries, snapshotOffset = 3)
      result.size shouldBe 2
      result(1) shouldBe newEntries(0)
    }
  }

  "LogOps.compact" should {

    "remove entries up to the given index" in {
      val log = Vector(
        LogEntry(1, 1, Command.Put("a", "1")),
        LogEntry(1, 2, Command.Put("b", "2")),
        LogEntry(2, 3, Command.Put("c", "3"))
      )
      val result = LogOps.compact(log, 2)
      result.size shouldBe 1
      result(0).index shouldBe 3
    }

    "return empty when compacting past the end" in {
      val log = Vector(LogEntry(1, 1, Command.Noop))
      LogOps.compact(log, 5) shouldBe empty
    }

    "return full log when compacting at index 0" in {
      val log = Vector(LogEntry(1, 1, Command.Noop))
      LogOps.compact(log, 0) shouldBe log
    }

    "handle snapshot offset correctly" in {
      val log = Vector(
        LogEntry(2, 4, Command.Put("d", "4")),
        LogEntry(2, 5, Command.Put("e", "5"))
      )
      val result = LogOps.compact(log, 4, snapshotOffset = 3)
      result.size shouldBe 1
      result(0).index shouldBe 5
    }
  }

  "LogOps.applyCommitted" should {

    "apply Put commands to the state machine" in {
      val log = Vector(
        LogEntry(1, 1, Command.Put("x", "10")),
        LogEntry(1, 2, Command.Put("y", "20"))
      )
      val (sm, applied) = LogOps.applyCommitted(Map.empty, log, 0, 2)
      sm shouldBe Map("x" -> "10", "y" -> "20")
      applied shouldBe 2
    }

    "apply Delete commands" in {
      val initial = Map("x" -> "10", "y" -> "20")
      val log = Vector(LogEntry(1, 1, Command.Delete("x")))
      val (sm, applied) = LogOps.applyCommitted(initial, log, 0, 1)
      sm shouldBe Map("y" -> "20")
      applied shouldBe 1
    }

    "skip Noop commands" in {
      val log = Vector(
        LogEntry(1, 1, Command.Noop),
        LogEntry(1, 2, Command.Put("a", "1"))
      )
      val (sm, applied) = LogOps.applyCommitted(Map.empty, log, 0, 2)
      sm shouldBe Map("a" -> "1")
      applied shouldBe 2
    }

    "only apply entries between lastApplied and commitIndex" in {
      val log = Vector(
        LogEntry(1, 1, Command.Put("a", "1")),
        LogEntry(1, 2, Command.Put("b", "2")),
        LogEntry(1, 3, Command.Put("c", "3"))
      )
      val (sm, applied) = LogOps.applyCommitted(Map("a" -> "1"), log, 1, 2)
      sm shouldBe Map("a" -> "1", "b" -> "2")
      applied shouldBe 2
    }

    "do nothing when lastApplied equals commitIndex" in {
      val log = Vector(LogEntry(1, 1, Command.Put("a", "1")))
      val (sm, applied) = LogOps.applyCommitted(Map("a" -> "1"), log, 1, 1)
      sm shouldBe Map("a" -> "1")
      applied shouldBe 1
    }

    "handle snapshot offset" in {
      val log = Vector(
        LogEntry(2, 4, Command.Put("d", "4")),
        LogEntry(2, 5, Command.Put("e", "5"))
      )
      val (sm, applied) = LogOps.applyCommitted(Map("c" -> "3"), log, 3, 5, snapshotOffset = 3)
      sm shouldBe Map("c" -> "3", "d" -> "4", "e" -> "5")
      applied shouldBe 5
    }

    "overwrite existing keys with Put" in {
      val log = Vector(
        LogEntry(1, 1, Command.Put("x", "old")),
        LogEntry(1, 2, Command.Put("x", "new"))
      )
      val (sm, _) = LogOps.applyCommitted(Map.empty, log, 0, 2)
      sm shouldBe Map("x" -> "new")
    }
  }

  "Command" should {
    "support Put, Delete, and Noop variants" in {
      Command.Put("k", "v") shouldBe a[Command.Put]
      Command.Delete("k") shouldBe a[Command.Delete]
      Command.Noop shouldBe Command.Noop
    }
  }

  "LogEntry" should {
    "store term, index, and command" in {
      val entry = LogEntry(3, 7, Command.Put("key", "val"))
      entry.term shouldBe 3
      entry.index shouldBe 7
      entry.command shouldBe Command.Put("key", "val")
    }
  }
