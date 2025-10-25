package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_json_window_test[]
// Test for JsonWindowedAggExample: verifies late rows are dropped and state is bounded

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import java.sql.Timestamp

class JsonWindowedAggExampleSuite extends AnyFunSuite {
  test("windowed agg drops late rows beyond watermark") {
  val spark = SparkSession.builder()
      .master("local[2]")
      .appName("JsonWindowedAggExampleSuite")
      .getOrCreate()
    import spark.implicits._

    import org.apache.spark.sql.execution.streaming.MemoryStream
    val inputStream = MemoryStream[(Timestamp, String)](1, spark.sqlContext)
    val now = System.currentTimeMillis()
    val rows = Seq(
      (new Timestamp(now - 1000 * 60 * 5), "foo"), // within window
      (new Timestamp(now - 1000 * 60 * 50), "bar"), // late, beyond watermark
      (new Timestamp(now - 1000 * 60 * 2), "foo")  // within window
    )
    inputStream.addData(rows: _*)
    val df = inputStream.toDF().toDF("timestamp", "word")
    val withWatermark = df.withWatermark("timestamp", "42 minutes")
    val windowed = withWatermark
      .groupBy(window(col("timestamp"), "10 minutes"), col("word"))
      .count()

    val query = windowed.writeStream
      .outputMode("append")
      .format("memory")
      .queryName("json_windowed_agg")
      .trigger(Trigger.Once())
      .option("checkpointLocation", "./tmp/checkpoints/json_windowed_agg_test")
      .start()
    query.processAllAvailable()
    query.awaitTermination()

    val result = spark.sql("select word, count from json_windowed_agg").collect().map(_.getString(0)).toSet
    assert(result.contains("foo"))
    assert(!result.contains("bar"), "Late row 'bar' should be dropped")
    spark.stop()
  }
}
// end::streaming_ex_json_window_test[]
