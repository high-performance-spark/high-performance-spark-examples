package com.highperformancespark.examples.structuredstreaming

// tag::basic_ex_with_checkpoint[]
// Basic socket wordcount with checkpointing
// Non-replayable source: socket is not fault tolerant, may lose data if restarted
// Checkpointing: use a durable path for production, e.g., HDFS or cloud storage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BasicSocketWordCountWithCheckpoint {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BasicSocketWordCountWithCheckpoint")
      .master("local[2]")
      .getOrCreate()

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
      .option("checkpointLocation", "./tmp/checkpoints/basic_socket_wordcount") // Use a durable path in production
      .start()

    query.awaitTermination()
  }
}
// end::basic_ex_with_checkpoint[]
