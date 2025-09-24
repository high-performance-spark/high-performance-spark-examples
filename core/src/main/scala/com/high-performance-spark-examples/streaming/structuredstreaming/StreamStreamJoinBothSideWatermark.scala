package com.highperformancespark.examples.structuredstreaming

// tag::stream_stream_join_basic_both_side_watermark[]
// Stream-stream join with watermark on both sides
// State can be cleaned up

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamStreamJoinBothSideWatermark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("StreamStreamJoinBothSideWatermark")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val left = spark.readStream
      .format("memory")
      .load()
      .withWatermark("timestamp", "10 minutes")
    val right = spark.readStream
      .format("memory")
      .load()
      .withWatermark("timestamp", "10 minutes")

    val joined = left.join(
      right,
      expr("left.timestamp >= right.timestamp - interval 5 minutes AND left.timestamp <= right.timestamp + interval 5 minutes AND left.key = right.key")
    )

    val query = joined.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "./tmp/checkpoints/stream_stream_join_both_side_watermark")
      .start()
    query.awaitTermination()
  }
}
// end::stream_stream_join_basic_both_side_watermark[]
