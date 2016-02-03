/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark._
import org.apache.spark.rdd._

object Accumulators {
  /**
   * Compute the total fuzzyness with an accumulator while generating an id and zip pair for sorting
   */
  //tag::sumFuzzyAcc[]
  def computeTotalFuzzyNess(sc: SparkContext, rdd: RDD[RawPanda]): (RDD[(String, Long)], Double) = {
    val acc = sc.accumulator(0.0) // Create an accumulator with the initial value of 0.0
    val transformed = rdd.map{x => acc += x.attributes(0); (x.zip, x.id)}
    // accumulator still has zero value
    transformed.count() // force evaluation
    // Note: This example is dangerous since the transformation may be evaluated multiple times
    (transformed, acc.value)
  }
  //end::sumFuzzyAcc[]

  /**
   * Compute the max fuzzyness with an accumulator while generating an id and zip pair for sorting
   */
  //tag::maxFuzzyAcc[]
  def computeMaxFuzzyNess(sc: SparkContext, rdd: RDD[RawPanda]): (RDD[(String, Long)], Double) = {
    object MaxDoubleParam extends AccumulatorParam[Double] {
      override def zero(initValue: Double) = initValue
      override def addInPlace(r1: Double, r2: Double): Double = {
        Math.max(r1, r2)
      }
    }
    // Create an accumulator with the initial value of Double.MinValue
    val acc = sc.accumulator(Double.MinValue)(MaxDoubleParam)
    val transformed = rdd.map{x => acc += x.attributes(0); (x.zip, x.id)}
    // accumulator still has Double.MinValue
    transformed.count() // force evaluation
    // Note: This example is dangerous since the transformation may be evaluated multiple times
    (transformed, acc.value)
  }
  //end::maxFuzzyAcc[]
}
