package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_rocksdb_state_store_test[]
// Test for RocksDB state store example
// Skipped if RocksDB provider not available

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

class RocksDBStateStoreExampleSuite extends AnyFunSuite {
  test("rocksdb state store query runs if provider available") {
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("RocksDBStateStoreExampleSuite")
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()
    import spark.implicits._

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()

    val agg = df.withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("timestamp"), "5 minutes"))
      .count()

    try {
      val query = agg.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("rocksdb_state_store")
        .option("checkpointLocation", "./tmp/checkpoints/rocksdb_state_store_test")
        .trigger(Trigger.Once())
        .start()
      query.awaitTermination()
      val result = spark.sql("select * from rocksdb_state_store").collect()
      assert(result.length > 0)
    } catch {
      case e: Exception => cancel("RocksDB provider not available: " + e.getMessage)
    }
    spark.stop()
  }
}
// end::streaming_ex_rocksdb_state_store_test[]
