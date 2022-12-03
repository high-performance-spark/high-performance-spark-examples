/**
 * Simple tests for tokenization
 */
package com.highperformancespark.examples.tokenize

import java.lang.Thread

import org.apache.spark.streaming._

import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite

class SampleTokenizeSuite extends FunSuite with SharedSparkContext {
  val input = List("hi holden", "I like coffee")
  val expected = List("hi", "holden", "I", "like", "coffee")

  test("test the difficult to test one") {
    val inputRDD = sc.parallelize(input)
    val result = SampleTokenize.difficultTokenizeRDD(inputRDD).collect()
    assert(result.toList == expected)
  }

  test("test the easy to test one like the difficult one") {
    val inputRDD = sc.parallelize(input)
    val result = SampleTokenize.tokenizeRDD(inputRDD).collect()
    assert(result.toList == expected)
  }

  test("test the easy inner function - note no SC needed") {
    assert(SampleTokenize.tokenize("hi holden").toList == List("hi", "holden"))
  }
}
