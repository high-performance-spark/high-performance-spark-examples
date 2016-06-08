/**
 * Test our simple JNI
 */
package com.highperformancespark.examples.ffi

import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalatest.Matchers._


class PipeExampleSuite extends FunSuite with SharedSparkContext with Checkers {
  ignore("commentors on a pr") {
    val rdd = sc.parallelize(List(12883))
    val expected = (12883, List("SparkQA", "srowen"))
    val result = PipeExample.lookupUserPRS(sc, rdd)
    assert(expected === result.collect()(0))
  }
}
