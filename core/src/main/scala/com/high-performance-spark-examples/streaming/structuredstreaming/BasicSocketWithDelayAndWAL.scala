package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_basic_with_delay_and_wal[]
// Socket example with WAL and artificial delay
// WAL helps with recovery, but race conditions may still occur

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object BasicSocketWithDelayAndWAL {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder()
      .appName("BasicSocketWithDelayAndWAL")
      .master("local[2]")
      .config("spark.sql.streaming.checkpointLocation", "./tmp/checkpoints/socket_with_delay_and_wal")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", "true")
      .load()

    val words = lines.select(explode(split(col("value"), " ")).alias("word"), col("timestamp"))
    val counts = words.groupBy("word").count()

    val query = counts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "./tmp/checkpoints/socket_with_delay_and_wal")
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        Thread.sleep(500) // artificial delay
        batchDF.show()
      }
      .start()

    query.awaitTermination()
  }
}
// end::streaming_ex_basic_with_delay_and_wal[]
