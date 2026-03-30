/**
 * Show skipped stages for better understanding.
 */
package com.highperformancespark.examples

import org.apache.spark._
import org.apache.spark.rdd.RDD

object Skipped {
  def shuffleSkip(sc: SparkContext, inputRDD: RDD[String]): RDD[String] = {
    val byLength = inputRDD.map(x => (x.length, x))
    val grouped = byLength.groupByKey()
    val groupedRepart = grouped.repartition(10).map((x, k) => k.length)
    // Create the shuffle file
    val countGrouped = groupedRepart.count()
    val repart = grouped.repartiton(10)
    val combined = countGrouped.join(repart)
    combined
  }
}
