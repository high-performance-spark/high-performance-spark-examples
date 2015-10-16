package com.highperformancespark.examples.goldilocks

import org.apache.spark._
import org.apache.spark.sql.SQLContext
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

  val inputList = List(GoldiLocksRow(0.0, 4.5, 7.7, 5.0),
    GoldiLocksRow(1.0, 5.5, 6.7, 6.0),
    GoldiLocksRow(2.0, 5.5, 1.5, 7.0),
    GoldiLocksRow(3.0, 5.5, 0.5, 7.0),
    GoldiLocksRow(4.0, 5.5, 0.5, 8.0)
  )

  test("Goldi locks first try ") {
    val sqlContext = new SQLContext(sc)
    val input = sqlContext.createDataFrame(inputList)
    val secondAndThird = GoldiLocksFirstTry.findQuantiles(input, targetRanks = List(2L, 3L))
    val expectedResult = Map[Int, Set[Double]](
      0 -> Set(1.0, 2.0),
      1 -> Set(5.5, 5.5),
      2 -> Set(0.5, 1.5),
      3 -> Set(6.0, 7.0))
    secondAndThird.foreach(x => println( x._1 +"," + x._2.mkString(" ")))
     assert(expectedResult.forall{case ((index, expectedRanks)) =>
      secondAndThird.get(index).get.toSet.equals(expectedRanks)})
  }

  test("GoldiLocks With Hashmap ") {
    val sqlContext = new SQLContext(sc)
    val input = sqlContext.createDataFrame(inputList)
    val secondAndThird = GoldiLocksWithHashMap.findQuantiles(input, targetRanks = List(2L, 3L))
    val expectedResult = Map[Int, Set[Double]](
      0 -> Set(1.0, 2.0),
      1 -> Set(5.5, 5.5),
      2 -> Set(0.5, 1.5),
      3 -> Set(6.0, 7.0))
    secondAndThird.foreach(x => println( x._1 +"," + x._2.mkString(" ")))
    assert(expectedResult.forall{case ((index, expectedRanks)) =>
      secondAndThird.get(index).get.toSet.equals(expectedRanks)})
  }

  override def afterAll() {
    // We clear the driver port so that we don't try and bind to the same port on restart
    sc.stop()
    System.clearProperty("spark.driver.port")
    _sc = null
    super.afterAll()
  }
}
// end::MAGIC_PANDA[]


  case class GoldiLocksRow(pandaId : Double, softness : Double, fuzzyness : Double, size : Double  )