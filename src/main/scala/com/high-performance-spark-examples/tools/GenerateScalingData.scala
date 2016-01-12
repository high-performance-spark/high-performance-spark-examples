package com.highperformancespark.examples.tools

import com.highperformancespark.examples.dataframe.HappyPanda.PandaInfo

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.random.RandomRDDs

object GenerateScalingData {
  // tag::MAGIC_PANDA[]
  /**
   * Generate a Goldilocks data set. We expect the zip code to follow an exponential
   * distribution and the data its self to be normal
   */
  def generateGoldilocks(sc: SparkContext, elements: Long, size: Long): RDD[List[String]] = {
    val keyRDD = sc.parallelize(1L.to(size))
    val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000,  size = size).map(_.toInt)
    val valuesRDD = RandomRDDs.normalVectorRDD(sc, numRows = 1, numCols = size)
    keyRDD.zip(valuesRDD, valuesRDD).map{case (k, z, v) =>
      RawPanda(k, z, v(0) > 0.5, v)}
  }
  // end::MAGIC_PANDA[]
}
