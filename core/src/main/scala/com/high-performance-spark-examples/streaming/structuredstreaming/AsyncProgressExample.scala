package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_async_progress[]
// Micro-batch streaming with async progress tracking
// Behaves more like continuous; loses state/aggregation support

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object AsyncProgressExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("AsyncProgressExample")
      .master("local[2]")
      .config("spark.sql.streaming.asyncProgressTrackingEnabled", "true")
      .config("spark.sql.streaming.asyncProgressTrackingCheckpointIntervalMs", "5000")
      .getOrCreate()

    import spark.implicits._
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()

    val out = df.selectExpr("value as v")
    val query = out.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .option("checkpointLocation", "./tmp/checkpoints/async_progress")
      .start()

    query.awaitTermination()
  }
}
// end::streaming_ex_async_progress[]
