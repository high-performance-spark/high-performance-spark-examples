package com.highperformancespark.examples.tools

import com.highperformancespark.examples.dataframe.RawPanda

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.linalg.Vector

object GenerateScalingData {
  /**
   * Generate a Goldilocks data set. We expect the zip code to follow an exponential
   * distribution and the data its self to be normal
   *
   * Note: May generate less than number of requested rows due to different
   * distribution between
   *
   * partitions and zip being computed per partition.
   * @param rows number of rows in the RDD (approximate)
   * @param size number of value elements
   */
  def generateFullGoldilocks(sc: SparkContext, rows: Long, numCols: Int):
      RDD[RawPanda] = {
    val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000,  size = rows)
      .map(_.toInt.toString)
    val valuesRDD = RandomRDDs.normalVectorRDD(
      sc, numRows = rows, numCols = numCols)
      .repartition(zipRDD.partitions.size)
    val keyRDD = sc.parallelize(1L.to(rows), zipRDD.getNumPartitions)
    keyRDD.zipPartitions(zipRDD, valuesRDD){
      (i1, i2, i3) =>
      new Iterator[(Long, String, Vector)] {
        def hasNext: Boolean = (i1.hasNext, i2.hasNext, i3.hasNext) match {
          case (true, true, true) => true
          case (false, false, false) => false
            // Note: this is "unsafe" (we throw away data when one of
            // the partitions has run out).
          case _ => false
        }
        def next(): (Long, String, Vector) = (i1.next(), i2.next(), i3.next())
      }
    }.map{case (k, z, v) =>
      RawPanda(k, z, "giant", v(0) > 0.5, v.toArray)}
  }

  /**
   * Transform it down to just the data used for the benchmark
   */
  def generateMiniScale(sc: SparkContext, rows: Long, numCols: Int):
      RDD[(Int, Double)] = {
    generateFullGoldilocks(sc, rows, numCols)
      .map(p => (p.zip.toInt, p.attributes(0)))
  }

  /**
   * Transform it down to just the data used for the benchmark
   */
  def generateMiniScaleRows(sc: SparkContext, rows: Long, numCols: Int):
      RDD[Row] = {
    generateMiniScale(sc, rows, numCols).map{case (zip, fuzzy) => Row(zip, fuzzy)}
  }

  // tag::MAGIC_PANDA[]
  /**
   * Generate a Goldilocks data set all with the same id.
   * We expect the zip code to follow an exponential
   * distribution and the data its self to be normal.
   * Simplified to avoid a 3-way zip.
   *
   * Note: May generate less than number of requested rows due to
   * different distribution between partitions and zip being computed
   * per partition.
   */
  def generateGoldilocks(sc: SparkContext, rows: Long, numCols: Int):
      RDD[RawPanda] = {
    val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000,  size = rows)
      .map(_.toInt.toString)
    val valuesRDD = RandomRDDs.normalVectorRDD(
      sc, numRows = rows, numCols = numCols)
    zipRDD.zip(valuesRDD).map{case (z, v) =>
      RawPanda(1, z, "giant", v(0) > 0.5, v.toArray)
    }
  }
  // end::MAGIC_PANDA[]
}
