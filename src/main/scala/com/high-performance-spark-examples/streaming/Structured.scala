package com.highperformancespark.examples.structuredstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._


object Structured {
  def write(counts: Dataset[_]) = {
    //tag::writeComplete[]
    val query = counts.writeStream.outputMode(OutputMode.Complete())
      .format("parquet").queryName("pandas").start()
    //end::writeComplete[]
  }
}
