/**
 * Basic tests for our MLlib examples
 */
package com.highperformancespark.examples.mllib

import com.highperformancespark.examples.dataframe.RawPanda

import com.holdenkarau.spark.testing._

import org.scalatest.FunSuite


import org.apache.spark.mllib.linalg.{Vector => SparkVector}

class GoldilocksMLlibSuite extends FunSuite with SharedSparkContext {
  val rps = List(
    RawPanda(1L, "94110", "giant", true, Array(0.0, 0.0)),
    RawPanda(2L, "94110", "giant", false, Array(0.0, 3.0)),
    RawPanda(3L, "94110", "giant", true, Array(0.0, 2.0)))

  test("boolean to double") {
    assert(1.0 === GoldilocksMLlib.booleanToDouble(true))
    assert(0.0 === GoldilocksMLlib.booleanToDouble(false))
  }

  test("encoding") {
    val input = sc.parallelize(rps)
    val points = GoldilocksMLlib.toLabeledPointDense(input)
    assert(points.count() == 3)
    assert(points.filter(_.label != 0.0).count() == 2)
  }

  test("lookup table") {
    val input = sc.parallelize(List("hi", "bye", "coffee", "hi"))
    val table = GoldilocksMLlib.createLabelLookup(input)
    assert(table.size == 3)
  }

}
