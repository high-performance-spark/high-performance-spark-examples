package com.highperformancespark.examples.tools

import scala.collection.immutable.HashSet

import com.highperformancespark.examples.dataframe.RawPanda

import org.apache.spark._
import org.apache.spark.rdd.RDD

//tag::loggerImport[]
import com.typesafe.scalalogging.LazyLogging
//end::loggerImport[]

object FilterInvalidPandas extends LazyLogging {

  def filterInvalidPandas(sc: SparkContext, invalidPandas: List[Long],
    input: RDD[RawPanda]) = {
    //tag::broadcast[]
    val invalid = HashSet() ++ invalidPandas
    val invalidBroadcast = sc.broadcast(invalid)
    input.filter{panda => !invalidBroadcast.value.contains(panda.id)}
    //end::broadcast[]
  }

  def filterInvalidPandasWithLogs(sc: SparkContext, invalidPandas: List[Long],
    input: RDD[RawPanda]) = {
    //tag::broadcastAndLog[]
    val invalid = HashSet() ++ invalidPandas
    val invalidBroadcast = sc.broadcast(invalid)
    def keepPanda(pandaId: Long) = {
      if (invalidBroadcast.value.contains(pandaId)) {
        logger.debug(s"Invalid panda ${pandaId} discovered")
        false
      } else {
        true
      }
    }
    input.filter{panda => keepPanda(panda.id)}
    //end::broadcastAndLog[]
  }
}
