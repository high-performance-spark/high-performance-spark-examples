package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_basic_test[]
// Test for BasicSocketWordCount using memory source and sink
// Hermetic: does not require real socket

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

class BasicSocketWordCountSuite extends AnyFunSuite {
  test("wordcount works with memory stream source") {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("BasicSocketWordCountSuite")
      .getOrCreate()
    import spark.implicits._

    // Use MemoryStream for hermetic streaming input
    import org.apache.spark.sql.execution.streaming.MemoryStream
    val inputStream = MemoryStream[String](1, spark.sqlContext)
    inputStream.addData("hello world hello")
    val df = inputStream.toDF().toDF("value")
    val words = df.select(explode(split(col("value"), " ")).alias("word"))
    val counts = words.groupBy("word").count()

    val query = counts.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("wordcount")
      .trigger(Trigger.Once())
      .start()
    query.processAllAvailable() // Ensures all data is processed for MemoryStream
    query.stop()

    val result = spark.sql("select word from wordcount").collect().map(_.getString(0)).toSet
    assert(result == Set("hello", "world"))
    spark.stop()
  }
}
// end::streaming_ex_basic_test[]
