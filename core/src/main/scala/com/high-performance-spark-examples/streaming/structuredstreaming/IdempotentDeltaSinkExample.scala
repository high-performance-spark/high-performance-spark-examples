package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_idempotent_sink[]
// Idempotent sink example with Delta
// Idempotency via dedupe/transactions; see Delta docs for caveats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object IdempotentDeltaSinkExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("IdempotentDeltaSinkExample")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()

    val out = df.selectExpr("value as id", "timestamp")
    val query = out.writeStream
      .outputMode("update")
      .format("delta")
      .option("checkpointLocation", "./tmp/checkpoints/idempotent_delta_sink")
      .option("path", "./tmp/delta/idempotent_sink")
      .start()

    query.awaitTermination()
  }
}
// end::streaming_ex_idempotent_sink[]
