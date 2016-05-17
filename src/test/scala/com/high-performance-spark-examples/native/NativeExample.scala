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

class NativeExampleSuite extends FunSuite with SharedSparkContext with Checkers {
  test("local sum") {
  //def magic2() {
    val input = Array(1, 2, 3)
    val sumMagic = new SumJNI()
    val result = sumMagic.sum(input)
    val expected = 6
    result === expected
  }

  //  test("super simple test") {
  def magic() {
    val input = sc.parallelize(List(("hi", Array(1, 2, 3))))
    val result = NativeExample.jniSum(input).collect()
    val expected = List(("hi", 6))
    result === expected
  }

  test("native call should find sum correctly") {
    val property = forAll(RDDGenerator.genRDD(sc)(Arbitrary.arbitrary[(String, Array[Int])])) {
      rdd => rdd.mapValues(_.sum).collect() === NativeExample.jniSum(rdd)
    }
    //check(property)
  }
}
