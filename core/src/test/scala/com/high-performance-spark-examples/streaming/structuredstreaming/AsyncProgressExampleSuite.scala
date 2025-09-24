package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_async_progress_test[]
// Test for AsyncProgressExample: verifies query runs with async progress configs

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

class AsyncProgressExampleSuite extends AnyFunSuite {
  test("async progress query produces rows quickly") {
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("AsyncProgressExampleSuite")
      .config("spark.sql.streaming.asyncProgressTrackingEnabled", "true")
      .config("spark.sql.streaming.asyncProgressTrackingCheckpointIntervalMs", "5000")
      .getOrCreate()
    import spark.implicits._

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()

    val query = df.writeStream
      .outputMode("append")
      .format("memory")
      .queryName("async_progress")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", "./tmp/checkpoints/async_progress_test")
      .start()
    query.processAllAvailable()

    val result = spark.sql("select * from async_progress").collect()
    assert(result.length > 0, "Should produce at least one row quickly")
    spark.stop()
  }
}
// end::streaming_ex_async_progress_test[]
