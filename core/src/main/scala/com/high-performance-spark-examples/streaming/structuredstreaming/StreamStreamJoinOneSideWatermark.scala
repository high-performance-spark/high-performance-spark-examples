package com.highperformancespark.examples.structuredstreaming

// Stream-stream join with watermark only on left
// Still insufficient for bounded cleanup

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamStreamJoinOneSideWatermark {
  def streamStreamJoinDF(
      spark: SparkSession,
      stream1: DataFrame,
      stream2: DataFrame
  ) = {
    // tag::stream_stream_join_basic_one_side_watermark[]
    val left = stream1.alias("left").withWatermark("timestamp", "10 minutes")
    val right = stream2.alias("right")
    val joined = left.join(
      right,
      expr(
        "left.timestamp >= right.timestamp - interval 5 minutes AND left.timestamp <= right.timestamp + interval 5 minutes AND left.key = right.key"
      )
    )

    val query = joined.writeStream
      .outputMode("append")
      .format("console")
      .option(
        "checkpointLocation",
        "./tmp/checkpoints/stream_stream_join_one_side_watermark"
      )
      .start()
    query.awaitTermination()
    // end::stream_stream_join_basic_one_side_watermark[]
  }
}
