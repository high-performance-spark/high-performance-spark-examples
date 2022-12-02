/**
 * Test that the accumulator example computes stuff.
 */
package com.highperformancespark.examples.transformations

import com.highperformancespark.examples.dataframe.RawPanda

import com.holdenkarau.spark.testing._

import org.scalatest.FunSuite
import scala.collection.immutable.HashSet

class AccumulatorsTest extends FunSuite with SharedSparkContext {
  test("accumulator max should function") {
    val input = sc.parallelize(1.to(100)).map(x =>
      RawPanda(1L, "1", "red", true, Array(x.toDouble)))
    val (_, max) = Accumulators.computeMaxFuzzyNess(sc, input)
    assert(max === 100.0)
  }

  test("accumulator sum should function") {
    val input = sc.parallelize(1.to(100)).map(x =>
      RawPanda(1L, "1", "red", true, Array(x.toDouble)))
    val (_, sum) = Accumulators.computeTotalFuzzyNess(sc, input)
    assert(sum === 5050.0)
  }

  test("accumulator unique should function") {
    val input1 = sc.parallelize(1 to 100).map(x =>
      RawPanda(1L, "1", "red", true, Array(x.toDouble))
    )

    val input2 = sc.parallelize(1 to 100).map(x =>
      RawPanda(2L, "2", "blude", false, Array(x.toDouble))
    )

    val set = Accumulators.uniquePandas(sc, input1 ++ input2)
    assert(set == HashSet(2, 1))
  }
}
