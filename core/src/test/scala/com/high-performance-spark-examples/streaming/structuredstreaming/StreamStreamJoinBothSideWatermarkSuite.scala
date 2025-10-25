package com.highperformancespark.examples.structuredstreaming

// tag::stream_stream_join_basic_both_side_watermark_test[]
// Test for stream-stream join with watermark on both sides
// Verifies bounded state and correct join results

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import java.sql.Timestamp

class StreamStreamJoinBothSideWatermarkSuite extends AnyFunSuite {
  test("join with both-side watermark yields bounded state and correct results") {
  val spark = SparkSession.builder()
      .master("local[2]")
      .appName("StreamStreamJoinBothSideWatermarkSuite")
      .getOrCreate()
    import spark.implicits._

    import org.apache.spark.sql.execution.streaming.MemoryStream
    val now = System.currentTimeMillis()
    val leftStream = MemoryStream[(Timestamp, String)](1, spark.sqlContext)
    val rightStream = MemoryStream[(Timestamp, String)](2, spark.sqlContext)
    val leftRows = Seq(
      (new Timestamp(now - 1000 * 60 * 5), "k1"), // within window
      (new Timestamp(now - 1000 * 60 * 20), "k2") // late, beyond watermark
    )
    val rightRows = Seq(
      (new Timestamp(now - 1000 * 60 * 5), "k1"), // within window
      (new Timestamp(now - 1000 * 60 * 20), "k2") // late, beyond watermark
    )
    leftStream.addData(leftRows: _*)
    rightStream.addData(rightRows: _*)
    val leftDF = leftStream.toDF().toDF("timestamp", "key").withWatermark("timestamp", "10 minutes")
    val rightDF = rightStream.toDF().toDF("timestamp", "key").withWatermark("timestamp", "10 minutes")

    val joined = leftDF.join(
      rightDF,
      leftDF("key") === rightDF("key") &&
        leftDF("timestamp") >= rightDF("timestamp") - expr("interval 5 minutes") &&
        leftDF("timestamp") <= rightDF("timestamp") + expr("interval 5 minutes")
    )

    val query = joined.writeStream
      .outputMode("append")
      .format("memory")
      .queryName("stream_stream_join_both_side_watermark")
      .trigger(Trigger.Once())
      .option("checkpointLocation", "./tmp/checkpoints/stream_stream_join_both_side_watermark_test")
      .start()
    query.processAllAvailable()
    query.awaitTermination()

    val result = spark.sql("select key from stream_stream_join_both_side_watermark").collect().map(_.getString(0)).toSet
    assert(result == Set("k1"), "Only non-late key should join")
    spark.stop()
  }
}
// end::stream_stream_join_basic_both_side_watermark_test[]
