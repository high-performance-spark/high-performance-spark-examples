package com.highperformancespark.examples.structuredstreaming

import java.sql.Timestamp
import java.nio.file.{Files, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.scalatest.funsuite.AnyFunSuite

// spark-testing-base
import com.holdenkarau.spark.testing.DatasetSuiteBase

final case class Ev(key: String, timestamp: Timestamp, v: Int)

class StreamStreamJoinBothSideWatermarkSpec
    extends AnyFunSuite
    with DatasetSuiteBase {

  import spark.implicits._

  private def ts(secs: Long): Timestamp =
    new Timestamp(secs * 1000L)

  private def splitAnd(e: Expression): Seq[Expression] = e match {
    case And(l, r) => splitAnd(l) ++ splitAnd(r)
    case other     => Seq(other)
  }

  private def joinedDF(leftIn: DataFrame, rightIn: DataFrame): DataFrame =
    StreamStreamJoinBothSideWatermark.streamStreamJoinDF(spark, leftIn, rightIn)

  /** Collect logical plan nodes of a given type. */
  private def collect[T <: LogicalPlan](p: LogicalPlan)(pf: PartialFunction[LogicalPlan, T]): Seq[T] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[T]
    p.foreach { node =>
      if (pf.isDefinedAt(node)) buf += pf(node)
    }
    buf.toSeq
  }

  private def normalizeExpr(e: Expression): String =
    e.sql
      .replaceAll("\\s+", " ")
      .trim

  test("streamStreamJoinDF sets watermarks on both sides with the configured delays") {
    val leftMem  = MemoryStream[Ev](1, spark.sqlContext)
    val rightMem = MemoryStream[Ev](2, spark.sqlContext)

    val df = joinedDF(leftMem.toDF(), rightMem.toDF())

    // Analyze the logical plan and assert the watermark nodes exist.
    val analyzed = df.queryExecution.analyzed

    val watermarks = collect(analyzed) { case w: EventTimeWatermark => w }
    assert(watermarks.size == 2, s"Expected 2 watermarks (one per side) but found ${watermarks.size}:\n$analyzed")

    // EventTimeWatermark(delay: CalendarInterval) shows in different ways across Spark versions.
    // The easiest robust check: inspect the sql/pretty output for the expected delays.
    val planStr = analyzed.toString().toLowerCase
    assert(planStr.contains("10 seconds") || planStr.contains("600000"), s"Expected left watermark 10 seconds in plan:\n$analyzed")
    assert(planStr.contains("5 seconds")  || planStr.contains("300000"), s"Expected right watermark 5 seconds in plan:\n$analyzed")
  }

  test("streamStreamJoinDF join condition is key equality AND timestamp within ±5 seconds") {
    val leftMem  = MemoryStream[Ev](3, spark.sqlContext)
    val rightMem = MemoryStream[Ev](4, spark.sqlContext)

    val df = joinedDF(leftMem.toDF(), rightMem.toDF())
    val analyzed = df.queryExecution.analyzed

    val joins = collect(analyzed) { case j: Join => j }
    assert(joins.nonEmpty, s"Expected a Join node in plan but found none:\n$analyzed")

    // There should be exactly one join at the top in this simple pipeline.
    val j = joins.head
    val cond = j.condition.getOrElse {
      throw new AssertionError(s"Join node had no condition:\n$j")
    }

    // We don’t want to match on attribute IDs, so we match on the *shape* of the condition.
    // Condition should be: left.key = right.key AND left.ts >= right.ts - 5m AND left.ts <= right.ts + 5m
    def isKeyEq(e: Expression): Boolean = e match {
      case EqualTo(l, r) =>
        normalizeExpr(l).endsWith(".key") && normalizeExpr(r).endsWith(".key")
      case _ => false
    }

    def isLowerBound(e: Expression): Boolean = e match {
      case GreaterThanOrEqual(l, r) =>
        normalizeExpr(l).endsWith(".timestamp") &&
          (normalizeExpr(r).contains("interval 5 seconds") || normalizeExpr(r).contains("300"))
      case _ => false
    }

    def isUpperBound(e: Expression): Boolean = e match {
      case LessThanOrEqual(l, r) =>
        normalizeExpr(l).endsWith(".timestamp") &&
          (normalizeExpr(r).contains("interval 5 seconds") || normalizeExpr(r).contains("300"))
      case _ => false
    }

    val conjuncts = splitAnd(cond)
    assert(conjuncts.size == 3, s"Expected 3 conjuncts but got ${conjuncts.size}: ${conjuncts.map(_.sql)}")

    assert(conjuncts.exists(isKeyEq),       s"Missing key equality conjunct in: ${conjuncts.map(_.sql)}")
    assert(conjuncts.exists(isLowerBound),  s"Missing lower-bound conjunct in: ${conjuncts.map(_.sql)}")
    assert(conjuncts.exists(isUpperBound),  s"Missing upper-bound conjunct in: ${conjuncts.map(_.sql)}")
  }

  test("watermark advancement changes results: an old pair joins before WM advance but not after") {
    val leftMem  = MemoryStream[Ev](5, spark.sqlContext)
    val rightMem = MemoryStream[Ev](6, spark.sqlContext)

    val outName = "ssjbw_behavior"
    val chk = Files.createTempDirectory("chk-ssjbw-behavior").toString

    val q = joinedDF(leftMem.toDF(), rightMem.toDF())
      .writeStream
      .format("memory")
      .queryName(outName)
      .outputMode("append")
      .option("checkpointLocation", chk)
      .start()

    // Baseline: old-ish pair *does* join (because we haven't advanced watermarks yet)
    leftMem.addData(Ev("A", ts(20), 1))
    rightMem.addData(Ev("A", ts(22), 2))
    q.processAllAvailable()
    val baselineCount = spark.table(outName).count()
    assert(baselineCount == 1, s"Expected baseline join count = 1, got $baselineCount")

    // Advance both watermarks far ahead
    // left watermark is 10s -> event at 200 => watermark ~ 190
    // right watermark is 5s -> event at 200 => watermark ~ 195
    leftMem.addData(Ev("A", ts(200), 3))
    rightMem.addData(Ev("A", ts(200), 4))
    q.processAllAvailable()

    // Now inject an even older pair that *would* satisfy the join condition,
    // but should be dropped as too-late relative to the watermarks.
    leftMem.addData(Ev("A", ts(30), 5))
    rightMem.addData(Ev("A", ts(32), 6))
    q.processAllAvailable()

    val afterCount = spark.table(outName).count()
    assert(afterCount == 1, s"Expected no additional output after watermark advance; count stayed 1 but got $afterCount")

    q.stop()
  }

  test("streamStreamJoin starts a query and creates checkpoint data at the configured relative path") {
    val leftMem  = MemoryStream[Ev](7, spark.sqlContext)
    val rightMem = MemoryStream[Ev](8, spark.sqlContext)

    // streamStreamJoin hardcodes "./tmp/checkpoints/stream_stream_join_both_side_watermark"
    // Make that predictable by running with a temp working dir.
    val tmpDir: Path = Files.createTempDirectory("ssjbw-cwd")
    val oldUserDir = System.getProperty("user.dir")
    System.setProperty("user.dir", tmpDir.toString)
    try {
      val q = StreamStreamJoinBothSideWatermark.streamStreamJoin(spark, leftMem.toDF(), rightMem.toDF())

      // Nudge it so it actually writes offsets/commits
      leftMem.addData(Ev("A", ts(10), 1))
      rightMem.addData(Ev("A", ts(12), 2))
      q.processAllAvailable()

      val chkPath = tmpDir
        .resolve("tmp")
        .resolve("checkpoints")
        .resolve("stream_stream_join_both_side_watermark")

      assert(Files.exists(chkPath), s"Expected checkpoint dir to exist at $chkPath")
      // offsets/commits existence is a strong signal the query is using that checkpoint location
      assert(
        Files.exists(chkPath.resolve("offsets")) || Files.exists(chkPath.resolve("commits")),
        s"Expected checkpoint offsets/commits under $chkPath, found: ${chkPath.toFile.list().toSeq}"
      )

      q.stop()
    } finally {
      System.setProperty("user.dir", oldUserDir)
    }
  }
}
