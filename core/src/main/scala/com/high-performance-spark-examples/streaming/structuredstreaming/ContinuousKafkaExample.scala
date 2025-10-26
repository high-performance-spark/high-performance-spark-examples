package com.highperformancespark.examples.structuredstreaming

// tag::streaming_ex_continuous_kafka[]
// Continuous mode Kafka example
// Limitations: no aggregations/state, manual fault tolerance, Kafka is primary prod source/sink

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object ContinuousKafkaExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ContinuousKafkaExample")
      .master("local[2]")
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "input_topic")
      .load()

    val query = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "output_topic")
      .trigger(Trigger.Continuous("1 second"))
      .option("checkpointLocation", "./tmp/checkpoints/continuous_kafka")
      .start()

    query.awaitTermination()
  }
}
// end::streaming_ex_continuous_kafka[]
