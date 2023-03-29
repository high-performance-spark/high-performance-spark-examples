package com.highperformancespark.examples.tokenize

import org.apache.spark.rdd.RDD

object SampleTokenize {
  //tag::DIFFICULT[]
  def difficultTokenizeRDD(input: RDD[String]) = {
    input.flatMap(_.split(" "))
  }
  //end::DIFFICULT[]

  //tag::EASY[]
  def tokenizeRDD(input: RDD[String]) = {
    input.flatMap(tokenize)
  }

  protected[tokenize] def tokenize(input: String) = {
    input.split(" ")
  }
  //end::EASY[]
}
