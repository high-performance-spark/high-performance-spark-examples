package com.highperformancespark.examples.tools

import scala.collection.immutable.HashSet

import com.highperformancespark.examples.dataframe.RawPanda

import org.apache.spark._
import org.apache.spark.rdd.RDD

object FilterInvalidPandas {

  def filterInvalidPandas(sc: SparkContext, invalidPandas: List[Long], input: RDD[RawPanda]) = {
    val invalid = HashSet() ++ invalidPandas
    val invalidBroadcast = sc.broadcast(invalid)
    input.filter{panda => !invalidBroadcast.value.contains(panda.id)}
  }
}
