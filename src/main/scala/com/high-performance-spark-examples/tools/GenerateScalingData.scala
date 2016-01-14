package com.highperformancespark.examples.tools

import com.highperformancespark.examples.dataframe.HappyPanda.RawPanda

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.linalg.Vector

object GenerateScalingData {
  /**
   * Generate a Goldilocks data set. We expect the zip code to follow an exponential
   * distribution and the data its self to be normal
   * @param rows number of rows in the RDD
   * @param size number of value elements
   */
  def generateFullGoldilocks(sc: SparkContext, rows: Long, size: Int): RDD[RawPanda] = {
    val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000,  size = rows).map(_.toInt.toString)
    val valuesRDD = RandomRDDs.normalVectorRDD(sc, numRows = rows, numCols = size)
    val keyRDD = sc.parallelize(1L.to(rows), zipRDD.partitions.size)
    keyRDD.zipPartitions(zipRDD, valuesRDD){
      (i1, i2, i3) =>
      new Iterator[(Long, String, Vector)] {
        def hasNext: Boolean = (i1.hasNext, i2.hasNext, i3.hasNext) match {
          case (true, true, true) => true
          case (false, false, false) => false
          case _ => throw new SparkException("Can only zip RDDs with " +
            "same number of elements in each partition")
        }
        def next(): (Long, String, Vector) = (i1.next(), i2.next(), i3.next())
      }
    }.map{case (k, z, v) =>
      RawPanda(k, z, v(0) > 0.5, v.toArray)}
  }

  // tag::MAGIC_PANDA[]
  /**
   * Generate a Goldilocks data set all with the same id.
   * We expect the zip code to follow an exponential
   * distribution and the data its self to be normal.
   * Simplified to avoid a 3-way zip.
   */
  def generateGoldilocks(sc: SparkContext, elements: Long, size: Int): RDD[RawPanda] = {
    val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000,  size = size).map(_.toInt.toString)
    val valuesRDD = RandomRDDs.normalVectorRDD(sc, numRows = 1, numCols = size)
      zipRDD.zip(valuesRDD).map{case (z, v) =>
      RawPanda(1, z, v(0) > 0.5, v.toArray)}
  }
  // end::MAGIC_PANDA[]
}
