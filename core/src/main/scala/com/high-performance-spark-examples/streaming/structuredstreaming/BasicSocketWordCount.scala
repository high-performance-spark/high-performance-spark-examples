package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_basic[]
// Basic socket wordcount example for Structured Streaming
// Non-replayable source: socket is not fault tolerant, may lose data if restarted
// See book for more details

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BasicSocketWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BasicSocketWordCount")
      .master("local[2]")
      .getOrCreate()

    // Socket source: not replayable, not fault tolerant
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.select(explode(split(col("value"), " ")).alias("word"))
    val counts = words.groupBy("word").count()

    val query = counts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
// end::streaming_ex_basic[]
