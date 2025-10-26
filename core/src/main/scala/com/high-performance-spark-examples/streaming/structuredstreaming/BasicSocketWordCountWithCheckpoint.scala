package com.highperformancespark.examples.structuredstreaming

// Basic socket wordcount with checkpointing
// Non-replayable source: socket is not fault tolerant, may lose data if restarted
// Checkpointing: use a durable path for production, e.g., HDFS or cloud storage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BasicSocketWordCountWithCheckpoint {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BasicSocketWordCountWithCheckpoint")
      .master("local[2]")
      .getOrCreate()
    run(spark)
  }

  def run(spark: SparkSession): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.select(explode(split(col("value"), " ")).alias("word"))
    val counts = words.groupBy("word").count()

    // tag::basic_ex_with_checkpoint[]
    val query = counts.writeStream
      .outputMode("complete")
      .format("console")
    // Note: You can also set spark.sql.streaming.checkpointLocation on the SparkSession
      .option(
        "checkpointLocation",
        "checkpoints/basic_socket_wordcount"
      ) // Use a durable path in production
      .start()
    // end::basic_ex_with_checkpoint[]
    query.awaitTermination()
  }
}
