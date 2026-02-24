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
    assert(planStr.contains("10 seconds") || planStr.contains("1000"), s"Expected left watermark 10 seconds in plan:\n$analyzed")
    assert(planStr.contains("5 seconds")  || planStr.contains("5000"), s"Expected right watermark 5 seconds in plan:\n$analyzed")
  }

  test("watermark advancement changes results: an old pair joins before WM advance but not after") {
    val leftMem  = MemoryStream[Ev](5, spark.sqlContext)
    val rightMem = MemoryStream[Ev](6, spark.sqlContext)

    val outName = "ssjbw_behavior1"
    val chk = Files.createTempDirectory("chk-ssjbw1-behavior").toString

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

    // Advance both watermarks far ahead with mismatch keys.
    // left watermark is 10s -> event at 200 => watermark ~ 190
    // right watermark is 5s -> event at 200 => watermark ~ 195
    leftMem.addData(Ev("A", ts(200), 3))
    rightMem.addData(Ev("B", ts(200), 4))
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
}
