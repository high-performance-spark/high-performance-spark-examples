package com.highperformancespark.examples.goldilocks

import org.apache.spark.rdd.RDD

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite


class JoinTest extends FunSuite with SharedSparkContext {
  test("Hash join"){
    val keySet = "a, b, c, d, e, f, g".split(",")
    val smallRDD = sc.parallelize(keySet.map(letter => (letter, letter.hashCode)))
    val largeRDD: RDD[(String, Double)] =
      sc.parallelize(keySet.flatMap{ letter =>
        Range(1, 50).map(i => (letter, letter.hashCode() / i.toDouble))})
    val result: RDD[(String, (Double, Int))] =
      RDDJoinExamples.manualBroadCastHashJoin(
        largeRDD, smallRDD)
    val nativeJoin: RDD[(String, (Double, Int))] = largeRDD.join(smallRDD)

    assert(result.subtract(nativeJoin).count == 0)
  }
}
