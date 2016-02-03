/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.transformations

import com.highperformancespark.examples.dataframe.RawPanda

import com.holdenkarau.spark.testing._

import org.scalatest.FunSuite

class AccumulatorsTest extends FunSuite with SharedSparkContext {
  test("accumulator max should function") {
    val input = sc.parallelize(1.to(100)).map(x => RawPanda(1L, "1", "red", true, Array(x.toDouble)))
    val (_, max) = Accumulators.computeMaxFuzzyNess(sc, input)
    assert(max === 100.0)
  }

  test("accumulator sum should function") {
    val input = sc.parallelize(1.to(100)).map(x => RawPanda(1L, "1", "red", true, Array(x.toDouble)))
    val (_, sum) = Accumulators.computeTotalFuzzyNess(sc, input)
    assert(sum === 5050.0)
  }
}
