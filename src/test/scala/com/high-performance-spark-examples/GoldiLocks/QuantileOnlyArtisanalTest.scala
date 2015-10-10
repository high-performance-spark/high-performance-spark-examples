package com.highperformancespark.examples.goldilocks

import org.apache.spark._
import org.apache.spark.rdd._

import org.scalatest.{BeforeAndAfterAll, FunSuite}

// tag::MAGIC_PANDA[]
class QuantileOnlyArtisanallTest extends FunSuite with BeforeAndAfterAll {
  @transient private var _sc: SparkContext = _
  def sc: SparkContext = _sc

  val conf = new SparkConf().setMaster("local[4]").setAppName("test")

  override def beforeAll() {
    _sc = new SparkContext(conf)
    super.beforeAll()
  }

  test("retrieve quantiles") {
    val input: RDD[((Double, Int), Long)] = sc.parallelize(
      List(((2.0, 1), 10L), ((1.0, 1), 5L), ((3.0, 1), 4L)))
    val result = new QuantileWithHashMap(input, List(1), List(1L, 6L)).findQuantiles()
    val expected = List(1.0, 2.0)
    assert(expected === result(1).toList)
  }

  override def afterAll() {
    // We clear the driver port so that we don't try and bind to the same port on restart
    System.clearProperty("spark.driver.port")
    _sc = null
    super.afterAll()
  }
}
// end::MAGIC_PANDA[]
