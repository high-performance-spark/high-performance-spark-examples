package com.highperformancespark.examples.structuredstreaming

// Stream-stream join with watermark on both sides
// State can be cleaned up

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object StreamStreamJoinBothSideWatermark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StreamStreamJoinBothSideWatermark")
      .master("local[2]")
      .getOrCreate()
  }

  def run(spark: SparkSession): Unit = {
    val left = spark.readStream
      .format("memory")
      .load()

    val right = spark.readStream
      .format("memory")
      .load()

    val query = streamStreamJoin(spark, left, right)
    query.awaitTermination()
  }

  def streamStreamJoinDF(spark: SparkSession, stream1: DataFrame, stream2: DataFrame): Dataset[Row] = {
    // Note the watermarks don't need to be the same, by default Spark will pick the min.
    // tag::stream_stream_join_basic_both_side_watermark[]
    // We provide the streams aliases so we can select specific keys.
    val left = stream1.alias("left").withWatermark("timestamp", "10 seconds")
    val right = stream2.alias("right").withWatermark("timestamp", "5 seconds")

    val joined = left.join(
      right,
      expr(
        "left.timestamp >= right.timestamp - interval 5 seconds " +
         " AND left.timestamp <= right.timestamp + interval 5 seconds " +
         " AND left.key = right.key"
      )
    )
    // end::stream_stream_join_basic_both_side_watermark[]
    joined
  }

  def streamStreamJoin(spark: SparkSession, stream1: DataFrame, stream2: DataFrame): StreamingQuery = {
    val joined = streamStreamJoinDF(spark, stream1, stream2)
    // tag::ex_with_checkpoin_at_writet[]
    val writer = joined.writeStream
      .outputMode("append")
      .format("console")
      .option(
        "checkpointLocation",
        "./tmp/checkpoints/stream_stream_join_both_side_watermark"
      )
    // end::ex_with_checkpoin_at_writet[]
    val query = writer.start()
    query
  }
}
