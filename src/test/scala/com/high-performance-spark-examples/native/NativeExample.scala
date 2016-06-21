/**
 * Test our simple JNI
 */
package com.highperformancespark.examples.ffi

import com.holdenkarau.spark.testing._
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalatest.Matchers._

class NativeExampleSuite extends FunSuite with SharedSparkContext with Checkers with RDDComparisons {

  test("local sum") {
    val input = Array(1, 2, 3)
    val sumMagic = new SumJNI()
    val result = sumMagic.sum(input)
    val expected = 6
    assert(result === expected)
  }

  test("super simple test") {
    val input = sc.parallelize(List(("hi", Array(1, 2, 3))))
    val result = NativeExample.jniSum(input).collect()
    val expected = List(("hi", 6))
    assert(result === expected)
  }

  test("native call should find sum correctly") {
    val property = forAll(RDDGenerator.genRDD[(String, Array[Int])](sc)(Arbitrary.arbitrary[(String, Array[Int])])) {
      rdd =>
        val expected = rdd.mapValues(_.sum)
        val result = NativeExample.jniSum(rdd)
        compareRDDWithOrder(expected, result).isEmpty
    }
    check(property)
  }

  test("JNA support") {
    val input = Array(1, 2, 3)
    assert(6 === SumJNA.sum(input, input.size))
  }

  test("JNA Fortran support") {
    val input = Array(1, 2, 3)
    assert(6 === SumFJNA.easySum(input.size, input))
  }
}
