package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_rocksdb_state_store[]
// Stateful aggregation with RocksDB state store
// Reduced memory pressure; still checkpointed externally

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object RocksDBStateStoreExample {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder()
      .appName("RocksDBStateStoreExample")
      .master("local[2]")
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()

    import spark.implicits._
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()

    val agg = df.withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("timestamp"), "5 minutes"))
      .count()

    val query = agg.writeStream
      .outputMode("update")
      .format("console")
      .option("checkpointLocation", "./tmp/checkpoints/rocksdb_state_store")
      .start()

    query.awaitTermination()
  }
}
// end::streaming_ex_rocksdb_state_store[]
