package com.highperformancespark.examples

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

class SkippedSuite extends AnyFunSuite with SharedSparkContext {
  override def appName: String = "skippedSuite"
  val input = List("hi holden", "I like coffee")

  test("test the difficult to test one") {
    val inputRDD = sc.parallelize(input)
    val result = Skipped.shuffleSkip(sc, inputRDD).count()
  }
}
