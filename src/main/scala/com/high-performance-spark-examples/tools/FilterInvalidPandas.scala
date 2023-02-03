package com.highperformancespark.examples.tools

import scala.collection.immutable.HashSet

import org.apache.spark._
import org.apache.spark.rdd.RDD

import com.highperformancespark.examples.dataframe.RawPanda
//tag::loggerImport[]
import org.apache.logging.log4j.LogManager
//end::loggerImport[]

object FilterInvalidPandas {

  def filterInvalidPandas(sc: SparkContext, invalidPandas: List[Long],
    input: RDD[RawPanda]) = {
    //tag::broadcast[]
    val invalid: HashSet[Long] = HashSet() ++ invalidPandas
    val invalidBroadcast = sc.broadcast(invalid)
    input.filter{panda => !invalidBroadcast.value.contains(panda.id)}
    //end::broadcast[]
  }

  def filterInvalidPandasWithLogs(sc: SparkContext, invalidPandas: List[Long],
    input: RDD[RawPanda]) = {
    //tag::broadcastAndLog[]
    val invalid: HashSet[Long] = HashSet() ++ invalidPandas
    val invalidBroadcast = sc.broadcast(invalid)
    def keepPanda(pandaId: Long) = {
      val logger = LogManager.getLogger("fart based logs")
      if (invalidBroadcast.value.contains(pandaId)) {
        logger.debug("hi")
        false
      } else {
        true
      }
    }
    input.filter{panda => keepPanda(panda.id)}
    //end::broadcastAndLog[]
  }
}

//tag::broadcastAndLogClass[]
class AltLog() {
  lazy val logger = LogManager.getLogger("fart based logs")
  def filterInvalidPandasWithLogs(sc: SparkContext, invalidPandas: List[Long],
      input: RDD[RawPanda]) = {
    val invalid: HashSet[Long] = HashSet() ++ invalidPandas
    val invalidBroadcast = sc.broadcast(invalid)
    def keepPanda(pandaId: Long) = {
      val logger = LogManager.getLogger("fart based logs")
      if (invalidBroadcast.value.contains(pandaId)) {
        logger.debug("hi")
        false
      } else {
        true
      }
    }
    input.filter{panda => keepPanda(panda.id)}
  }
}
//end::broadcastAndLogClass[]
