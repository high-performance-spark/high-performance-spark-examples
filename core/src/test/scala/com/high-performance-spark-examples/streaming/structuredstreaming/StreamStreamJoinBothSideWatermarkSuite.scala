package com.highperformancespark.examples.structuredstreaming

import java.sql.Timestamp
import java.nio.file.Files

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.funsuite.AnyFunSuite

// spark-testing-base
import com.holdenkarau.spark.testing.DatasetSuiteBase

final case class Ev(key: String, timestamp: Timestamp, v: Int)

class StreamStreamJoinBothSideWatermarkSTBSpec
    extends AnyFunSuite
    with DatasetSuiteBase {

  import spark.implicits._

  private def ts(mins: Long): Timestamp =
    new Timestamp(mins * 60L * 1000L) // epoch + minutes

  private def joinedDF(leftIn: DataFrame, rightIn: DataFrame): DataFrame = {
    StreamStreamJoinBothSideWatermark.streamStreamJoinDF(spark, leftIn, rightIn)
  }

  test("joins rows with same key within ±5 minutes") {
    val leftMem  = MemoryStream[Ev](1, spark.sqlContext)
    val rightMem = MemoryStream[Ev](2, spark.sqlContext)

    val outName = "stb_out_basic"
    val q = joinedDF(leftMem.toDF(), rightMem.toDF())
      .writeStream
      .format("memory")
      .queryName(outName)
      .outputMode("append")
      .option("checkpointLocation", Files.createTempDirectory("chk-basic").toString)
      .start()

    // Left @ 10, Right @ 12 -> within window and same key
    leftMem.addData(Ev("A", ts(10), 1))
    rightMem.addData(Ev("A", ts(12), 2))
    q.processAllAvailable()

    // Select a stable set of columns to compare
    val actual = spark.table(outName)
      .selectExpr("left.key as key", "left.timestamp as lt", "right.timestamp as rt")
      .as[(String, Timestamp, Timestamp)]

    val expected = Seq(("A", ts(10), ts(12))).toDS()

    assertDataFrameEquals(actual, expected)

    q.stop()
  }

  test("does not join when outside tolerance or key mismatch") {
    val leftMem  = MemoryStream[Ev](3, spark.sqlContext)
    val rightMem = MemoryStream[Ev](4, spark.sqlContext)

    val outName = "stb_out_filtering"
    val q = joinedDF(leftMem.toDF(), rightMem.toDF())
      .writeStream
      .format("memory")
      .queryName(outName)
      .outputMode("append")
      .option("checkpointLocation", Files.createTempDirectory("chk-filter").toString)
      .start()

    // Outside ±5 minutes (0 vs 7 -> 7 minutes apart)
    leftMem.addData(Ev("A", ts(0), 1))
    rightMem.addData(Ev("A", ts(7), 2))
    q.processAllAvailable()
    assert(spark.table(outName).isEmpty)

    // Within time but different keys
    rightMem.addData(Ev("B", ts(2), 9))
    q.processAllAvailable()
    assert(spark.table(outName).isEmpty)

    q.stop()
  }

  test("late data are dropped after both watermarks advance") {
    val leftMem  = MemoryStream[Ev](5, spark.sqlContext)
    val rightMem = MemoryStream[Ev](6, spark.sqlContext)

    val outName = "stb_out_late"
    val q = joinedDF(leftMem.toDF(), rightMem.toDF())
      .writeStream
      .format("memory")
      .queryName(outName)
      .outputMode("append")
      .option("checkpointLocation", Files.createTempDirectory("chk-late").toString)
      .start()

    // 1) Valid pair near t ~ 10..12
    leftMem.addData(Ev("A", ts(10), 1))
    rightMem.addData(Ev("A", ts(12), 2))
    q.processAllAvailable()
    assert(spark.table(outName).count() == 1)

    // 2) Advance BOTH watermarks far ahead:
    //    left WM delay 10m -> add t=100 -> WM ~ 90
    //    right WM delay 5m -> add t=100 -> WM ~ 95
    leftMem.addData(Ev("A", ts(100), 3))
    rightMem.addData(Ev("A", ts(100), 4))
    q.processAllAvailable()

    // 3) Inject events that would have joined in the past (t=20..22)
    //    but are now far older than both watermarks -> should be dropped.
    leftMem.addData(Ev("A", ts(20), 5))
    rightMem.addData(Ev("A", ts(22), 6))
    q.processAllAvailable()

    // Still only the first result
    assert(spark.table(outName).count() == 1)

    // Optional sanity: state metrics shouldn't balloon
    Option(q.lastProgress).foreach { p =>
      assert(p.stateOperators != null && p.stateOperators.nonEmpty)
      assert(p.stateOperators.head.numRowsTotal >= 0)
    }

    q.stop()
  }
}
