/** Show skipped stages for better understanding.
  */
package com.highperformancespark.examples

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Skipped {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = sparkSession.sparkContext
    val input = sc.parallelize(List("hi", "you"))
    shuffleSkipCache(sc, input)
  }
  def shuffleSkip(sc: SparkContext, inputRDD: RDD[String]): RDD[_] = {
    import sc._
    val byLength = inputRDD.map(x => (x.length, x))
    val grouped = byLength.groupByKey()
    val groupedRepart = grouped.repartition(10).mapValues(x => x.toList.length)
    // Create the shuffle file
    val countGrouped = groupedRepart.count()
    val repart = grouped.repartition(10)
    repart.count()
    val combined = groupedRepart.join(repart)
    combined
  }

  def shuffleSkipCache(sc: SparkContext, inputRDD: RDD[String]): RDD[_] = {
    import sc._
    val byLength = inputRDD.map(x => (x.length, x))
    val grouped = byLength.groupByKey()
    grouped.cache()
    grouped.count()
    val groupedRepart = grouped.mapValues(x => x.toList.length)
    // Create the shuffle file
    val countGrouped = groupedRepart.count()
    val repart = grouped.repartition(10)
    repart.count()
    val combined = groupedRepart.join(repart)
    combined
  }
}
