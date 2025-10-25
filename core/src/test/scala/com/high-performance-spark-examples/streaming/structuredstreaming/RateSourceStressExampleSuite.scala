package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_stress_rate_test[]
// Smoke test for rate source stress example

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

class RateSourceStressExampleSuite extends AnyFunSuite {
  test("rate source produces at least one row") {
  val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RateSourceStressExampleSuite")
      .getOrCreate()
    import spark.implicits._

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()

    val agg = df.selectExpr("value % 10 as bucket")
      .groupBy("bucket")
      .count()

    val query = agg.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("rate_stress")
      .trigger(Trigger.Once())
      .option("checkpointLocation", "./tmp/checkpoints/rate_stress_test")
      .start()
    query.awaitTermination()

    val result = spark.sql("select * from rate_stress").collect()
    assert(result.length > 0)
    spark.stop()
  }
}
// end::streaming_ex_stress_rate_test[]
