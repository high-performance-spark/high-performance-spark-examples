package com.highperformancespark.examples.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object JsonWindowedAggExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JsonWindowedAggExample")
      .master("local[2]")
      .getOrCreate()
    run(spark)
  }

  def run(spark: SparkSession): Unit = {
    val query = makeQuery(spark)
    query.awaitTermination()
  }

  /** Your original behavior (console sink, no watermark, continuous). */
  def makeQuery(spark: SparkSession): StreamingQuery = {
    makeQueryWith(
      spark,
      inputPath = "/tmp/json_input",
      checkpointDir = "/tmp/checkpoints/json_windowed_agg",
      outputFormat = "console",
      queryName = None,
      trigger = Trigger.ProcessingTime("5 seconds"),
      addWatermark = false
    )
  }

  /** Parametric builder used by tests (and optional batch-like runs). */
  def makeQueryWith(
      spark: SparkSession,
      inputPath: String,
      checkpointDir: String,
      outputFormat: String,
      queryName: Option[String],
      trigger: Trigger,
      addWatermark: Boolean
  ): StreamingQuery = {
    import spark.implicits._

    val df = spark.readStream
      .format("json")
      .schema("timestamp TIMESTAMP, word STRING")
      .load(inputPath)

    val base = if (addWatermark) df.withWatermark("timestamp", "5 minutes") else df
    val windowed = base
      .groupBy(window(col("timestamp"), "10 minutes"), col("word"))
      .count()

    val writer = windowed.writeStream
      .outputMode("append")
      .format(outputFormat)
      .option("checkpointLocation", checkpointDir)
      .trigger(trigger)

    val named = queryName.fold(writer)(n => writer.queryName(n))
    named.start()
  }
}
