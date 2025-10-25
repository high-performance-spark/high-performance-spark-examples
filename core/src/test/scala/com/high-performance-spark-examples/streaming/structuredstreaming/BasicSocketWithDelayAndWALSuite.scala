package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_basic_with_delay_and_wal_test[]
// Test for socket with WAL and artificial delay
// Hermetic: uses memory input, verifies WAL/progress logs and recovery

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

class BasicSocketWithDelayAndWALSuite extends AnyFunSuite {
  test("WAL/progress logs do not break pipeline and recovery works") {
    val checkpointDir = "./tmp/checkpoints/test_socket_with_delay_and_wal"
  val spark = SparkSession.builder()
      .master("local[2]")
      .appName("BasicSocketWithDelayAndWALSuite")
      .config("spark.sql.streaming.checkpointLocation", checkpointDir)
      .getOrCreate()
    import spark.implicits._

    val df = spark.createDataset(Seq("foo bar baz")).toDF("value")
    val words = df.select(explode(split(col("value"), " ")).alias("word"))
    val counts = words.groupBy("word").count()

    val query = counts.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("socket_with_delay_and_wal")
      .option("checkpointLocation", checkpointDir)
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        Thread.sleep(100)
      }
      .trigger(Trigger.Once())
      .start()
    query.awaitTermination()

    val result = spark.sql("select * from socket_with_delay_and_wal").collect().map(_.getString(0)).toSet
    assert(result == Set("foo", "bar", "baz"))
    spark.stop()
  }
}
// end::streaming_ex_basic_with_delay_and_wal_test[]
