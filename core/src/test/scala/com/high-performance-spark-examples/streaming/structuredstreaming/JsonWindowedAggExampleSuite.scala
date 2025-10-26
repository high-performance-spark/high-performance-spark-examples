package com.highperformancespark.examples.structuredstreaming

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.nio.file.Files
import java.sql.Timestamp

class JsonWindowedAggExampleFileIT extends AnyFunSuite {

  private def withSpark[T](f: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .appName("JsonWindowedAggExampleFileIT")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try f(spark) finally spark.stop()
  }

  test("file JSON source: sequential writes close windows via watermark (append mode)") {
    withSpark { spark =>
      import spark.implicits._

      val inputDir = Files.createTempDirectory("json-input-it").toFile.getAbsolutePath
      val chkDir   = Files.createTempDirectory("chk-it").toFile.getAbsolutePath
      val qName    = "json_winagg_mem_it"

      // Start the stream FIRST, using a periodic trigger and a watermark
      val q = JsonWindowedAggExample.makeQueryWith(
        spark,
        inputPath = inputDir,
        checkpointDir = chkDir,
        outputFormat = "memory",                       // assertable sink
        queryName = Some(qName),
        trigger = Trigger.ProcessingTime("250 milliseconds"),
        addWatermark = true                            // watermark = 5 minutes (set in builder)
      )

      // --- Batch 1: events in [10:00,10:10)
      Seq(
        ("2025-01-01 10:01:00", "hello"),
        ("2025-01-01 10:05:00", "hello"),
        ("2025-01-01 10:05:00", "world")
      ).map { case (ts, w) => (Timestamp.valueOf(ts), w) }
        .toDF("timestamp","word")
        .write.mode(SaveMode.Append).json(inputDir)

      // Let the stream pick up batch 1
      q.processAllAvailable() // ok in tests

      // Nothing should be emitted yet in append mode (window not closed)
      assert(spark.table(qName).count() == 0)

      // --- Batch 2: later event at 10:16 moves max event time to 10:16
      // Watermark = maxEventTime - 5m = 10:11 >= 10:10, so [10:00,10:10) closes and emits.
      Seq(("2025-01-01 10:16:00", "hello"))
        .map { case (ts, w) => (Timestamp.valueOf(ts), w) }
        .toDF("timestamp","word")
        .write.mode(SaveMode.Append).json(inputDir)

      q.processAllAvailable()

      val afterBatch2 = spark.table(qName)
        .select(
          date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").as("start"),
          date_format(col("window.end"),   "yyyy-MM-dd HH:mm:ss").as("end"),
          col("word"),
          col("count")
        )
        .collect()
        .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getLong(3)))
        .toSet

      val expectedAfterBatch2 = Set(
        ("2025-01-01 10:00:00", "2025-01-01 10:10:00", "hello", 2L),
        ("2025-01-01 10:00:00", "2025-01-01 10:10:00", "world", 1L)
      )
      assert(afterBatch2 == expectedAfterBatch2)

      // --- Batch 3: event at 10:26 closes [10:10,10:20)
      // New watermark = 10:21 >= 10:20 ⇒ the second window can now emit.
      Seq(("2025-01-01 10:26:00", "noop"))
        .map { case (ts, w) => (Timestamp.valueOf(ts), w) }
        .toDF("timestamp","word")
        .write.mode(SaveMode.Append).json(inputDir)

      q.processAllAvailable()

      val finalOut = spark.table(qName)
        .select(
          date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").as("start"),
          date_format(col("window.end"),   "yyyy-MM-dd HH:mm:ss").as("end"),
          col("word"),
          col("count")
        )
        .collect()
        .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getLong(3)))
        .toSet

      val expectedFinal = expectedAfterBatch2 ++ Set(
        ("2025-01-01 10:10:00", "2025-01-01 10:20:00", "hello", 1L)
      )
      assert(finalOut == expectedFinal)

      q.stop()
    }
  }
}
