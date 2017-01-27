package com.highperformancespark.examples.structuredstreaming

import scala.concurrent.duration._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._


object Structured {
  def load(inputPath: String, session: SparkSession): Dataset[_] = {
    //tag::loadSimple[]
    session.readStream.parquet(inputPath)
    //end::loadSimple[]
  }
  def write(counts: Dataset[_]) = {
    //tag::writeComplete[]
    val query = counts.writeStream.
      // Specify the output mode as Complete to support aggregations
      outputMode(OutputMode.Complete()).
      // Write out the result as parquet
      format("parquet").
      // Specify the interval at which new data will be picked up
      trigger(ProcessingTime(1.second)).
      queryName("pandas").start()
    //end::writeComplete[]
  }
}
