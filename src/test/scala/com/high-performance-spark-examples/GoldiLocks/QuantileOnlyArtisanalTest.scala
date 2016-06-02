package com.highperformancespark.examples.goldilocks

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

// tag::MAGIC_PANDA[]
class QuantileOnlyArtisanalTest extends FunSuite with SharedSparkContext {

  val inputList = List(
    GoldiLocksRow(0.0, 4.5, 7.7, 5.0),
    GoldiLocksRow(1.0, 5.5, 6.7, 6.0),
    GoldiLocksRow(2.0, 5.5, 1.5, 7.0),
    GoldiLocksRow(3.0, 5.5, 0.5, 7.0),
    GoldiLocksRow(4.0, 5.5, 0.5, 8.0)
  )

  test("Goldilocks first try ") {
    val sqlContext = new SQLContext(sc)
    val input = sqlContext.createDataFrame(inputList)
    val secondAndThird = GoldiLocksFirstTry.findRankStatistics(input, targetRanks = List(2L, 3L))
    val expectedResult = Map[Int, Set[Double]](
      0 -> Set(1.0, 2.0),
      1 -> Set(5.5, 5.5),
      2 -> Set(0.5, 1.5),
      3 -> Set(6.0, 7.0))
    secondAndThird.foreach(x => println( x._1 +"," + x._2.mkString(" ")))
    assert(expectedResult.forall{case ((index, expectedRanks)) =>
      secondAndThird.get(index).get.toSet.equals(expectedRanks)})
  }

  //tests the edge case in which one partition does not contain any of the elements in one column
  test("Goldilocks first try multiplePartitions") {
    import org.scalatest.PrivateMethodTester._
    val testData = sc.parallelize(List(1.0, 2.0, 3.0, 4.0).map(x => (x, x)), 3)
    val mapPartitions = testData.mapPartitionsWithIndex {
      case (index, iter) =>
        val key = if (index == 1) 1 else 0
          iter.map(x => (x._1, key))
    }

    val getColumnsFreqPerPartition = PrivateMethod[ Array[(Int, Array[Long])]]('getColumnsFreqPerPartition)
    val totals = GoldiLocksFirstTry invokePrivate getColumnsFreqPerPartition(mapPartitions, 2)

    totals.foreach(x => println(x._1 + " : " + x._2.mkString(" ")))
    val getRanksLocationsWithinEachPart =
      PrivateMethod[Array[(Int, List[(Int, Long)])]]('getRanksLocationsWithinEachPart)

    val locations = GoldiLocksFirstTry invokePrivate getRanksLocationsWithinEachPart(List(1L), totals, 2)
    locations.foreach(x => println(x._1 + " : " + x._2.mkString(" ")))

    //assert that there is nothing in the column with index 1 on the second partition
    assert(totals(1)._2(0) == 0 )

    val firstPartition = locations(0)._2
    //assertFirstPartitionOnlyContains a target rank for the for columnIndex 0, at index 1
    assert(firstPartition.toSet.equals(Set((0,1))) )

    //assertSecondPartition only contains rank for columnIndex 1, at index 1
    val secondPartition = locations(1)._2
    assert(secondPartition.toSet.equals(Set((1,1))) )

    //assert ThirdPartition contains no locations
    val thirdPartition = locations(2)._2
    assert(thirdPartition.toSet.equals(Set()))
    assert(locations.length == 3)
  }

  test("GoldiLocks With Hashmap ") {
    val sqlContext = new SQLContext(sc)
    val input = sqlContext.createDataFrame(inputList)
    val secondAndThird = GoldiLocksWithHashMap.findRankStatistics(input, targetRanks = List(2L, 3L))
    val expectedResult = Map[Int, Set[Double]](
      0 -> Set(1.0, 2.0),
      1 -> Set(5.5, 5.5),
      2 -> Set(0.5, 1.5),
      3 -> Set(6.0, 7.0))
    secondAndThird.foreach(x => println( x._1 +"," + x._2.mkString(" ")))
    assert(expectedResult.forall{case ((index, expectedRanks)) =>
      secondAndThird.get(index).get.toSet.equals(expectedRanks)})
  }

  test("Secondary Sort"){
    val data = sc.parallelize(Range.apply(0, 10)).flatMap( i => List(20.0, 30.0 , 40.0 ).map(x => ((x, i), 1L )))
    val r = SecondarySort.groupByKeyAndSortBySecondaryKey(data, 3)
    r.collect().foreach( v => println( v))
    val rSorted = r.collect().sortWith(
      lt = (a, b) => a._1.toDouble > b._1.toDouble )
    assert(r.collect().zipWithIndex.forall{
      case (((key, list), index )) => rSorted(index)._1.equals(key)
    })
  }

}
// end::MAGIC_PANDA[]

case class GoldiLocksRow(pandaId: Double, softness: Double, fuzziness: Double, size: Double)