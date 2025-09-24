package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_stress_rate[]
// Stress/benchmark example with rate source
// Tuning: batch interval, state vs executor memory, task startup overhead

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object RateSourceStressExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("RateSourceStressExample")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 20)
      .load()

    val agg = df.selectExpr("value % 10 as bucket")
      .groupBy("bucket")
      .count()

    val query = agg.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "./tmp/checkpoints/rate_stress")
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()

    query.awaitTermination()
  }
}
// end::streaming_ex_stress_rate[]
