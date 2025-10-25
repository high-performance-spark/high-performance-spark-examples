package com.highperformancespark.examples.structuredstreaming

// Windowed aggregation with watermark on JSON input
// Watermarking is needed to bound state and drop late data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object JsonWindowedAggExample {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder()
      .appName("JsonWindowedAggExample")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    // tag::streaming_ex_json_window[]
    val df = spark.readStream
      .format("json")
      .schema("timestamp TIMESTAMP, word STRING")
      .load("/tmp/json_input")

    val windowed = df
      .groupBy(window(col("timestamp"), "10 minutes"), col("word"))
      .count()
    // end::streaming_ex_json_window[]

    val query = windowed.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "./tmp/checkpoints/json_windowed_agg")
      .start()

    query.awaitTermination()
  }
}
