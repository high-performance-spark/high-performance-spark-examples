package com.highperformancespark.examples.structuredstreaming

// tag::basic_ex_with_checkpoint_test[]
// Test for BasicSocketWordCountWithCheckpoint using memory source/sink and checkpointing
// Hermetic: does not require real socket

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}

class BasicSocketWordCountWithCheckpointSuite extends AnyFunSuite {
  test("wordcount with checkpointing creates checkpoint dir and can restart") {
    val checkpointDir = "./tmp/checkpoints/test_basic_socket_wordcount"
  val spark = SparkSession.builder()
      .master("local[2]")
      .appName("BasicSocketWordCountWithCheckpointSuite")
      .getOrCreate()
    import spark.implicits._

    // Use MemoryStream for streaming input
    import org.apache.spark.sql.execution.streaming.MemoryStream
    val inputStream = MemoryStream[String](1, spark.sqlContext)
    val words = inputStream.toDF().select(explode(split(col("value"), " ")).alias("word"))
    val counts = words.groupBy("word").count()

    // Write to memory sink with checkpointing
    val query = counts.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("wordcount_checkpoint")
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.Once())
      .start()
    inputStream.addData("hello world hello")
    query.processAllAvailable()
    query.awaitTermination()

    assert(Files.exists(Paths.get(checkpointDir)), "Checkpoint directory should exist")

    // Simulate restart: start a new query with same checkpoint
    val query2 = counts.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("wordcount_checkpoint2")
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.Once())
      .start()
    inputStream.addData("hello world hello")
    query2.processAllAvailable()
    query2.awaitTermination()

    val result = spark.sql("select * from wordcount_checkpoint2").collect().map(_.getString(0)).toSet
    assert(result == Set("hello", "world"))
    spark.stop()
  }
}
// end::basic_ex_with_checkpoint_test[]
