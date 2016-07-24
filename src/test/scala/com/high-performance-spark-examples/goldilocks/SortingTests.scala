
package com.highperformancespark.examples.goldilocks

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import scala.reflect.ClassTag


class SortingTests extends FunSuite with SharedSparkContext {

  test("Test Sort by two keys"){

    val sortedData: Array[((Int, Char), Double)] = Range(0, 15).flatMap(  x =>
       Range(50, 100).map(i => (( x, i.toChar), Math.random()))
    ).toArray

    val unsorted = scramble(sc.parallelize(sortedData),2)
    val sortedSimple: Array[((Int, Char), Double)]  = unsorted.sortByKey().collect()

    assert(sortedSimple sameElements sortedData)
    }

  test("Panda Secondary Sort"){
    val pandaData: Array[(String, StreetAddress, Int, Double)] = Array(
      ("Morris", StreetAddress("Accra","Grove", 52 ), 84440, 0.0),
      ("Joe", StreetAddress("Accra","Grove", 52 ), 94440, 0.0),
      ("Kobe", StreetAddress("Accra","Grove", 52 ), 94440, 0.0),

      ("Morris", StreetAddress("Albany","Grove", 52 ), 84440, 0.0),
      ("Joe", StreetAddress("Albany","Grove", 52 ), 94440, 0.0),
      ("Kobe", StreetAddress("Albany","Grove", 52 ), 94440, 0.5),
      ("Morris", StreetAddress("Denver","Grove", 52 ), 84440, 0.5),
      ("Joe", StreetAddress("LA","Grove", 52 ), 94440, 0.5),
      ("Kobe", StreetAddress("LA","Grove", 52 ), 94440, 0.5),
      ("Joe", StreetAddress("SanFransisco","Grove", 52 ), 94440, 0.5),
      ("Kobe", StreetAddress("SanFransisco","Grove", 52 ), 94440, 0.5),
      ("Joe", StreetAddress("Seattle","Grove", 52 ), 84440, 0.5),
      ("Kobe", StreetAddress("Seattle","Grove", 52 ), 84440, 0.5),
      ("Lacy", StreetAddress("Seattle","Grove", 52 ), 84440, 0.5),
      ("Morris", StreetAddress("Seattle","Grove", 52 ), 84440, 0.5),
      ("Joe", StreetAddress("Seattle","Grove", 52 ), 94440, 0.5),
        ("Kobe", StreetAddress("Seattle","Grove", 52 ), 94440, 0.5),
        ("Lacy", StreetAddress("Seattle","Grove", 52 ), 94440, 0.5),
        ("Morris", StreetAddress("Seattle","Grove", 52 ), 94440, 0.5),
      ("Joe", StreetAddress("Tacoma","Grove", 52 ), 94440, 0.5),
      ("Kobe", StreetAddress("Tacoma","Grove", 52 ), 94440, 0.5),
      ("Lacy", StreetAddress("Tacoma","Grove", 52 ), 94440, 0.5),
      ("Morris", StreetAddress("Tacoma","Grove", 52 ), 94440, 0.5)
    )

    val unsorted = scramble(sc.parallelize(pandaData))
    val pandaSort = PandaSecondarySort.secondarySort(unsorted)
    pandaSort.zipWithIndex().collect.foreach{
      case (x, i) => assert(x == pandaData(i.toInt), "Element " + x + " is wrong")
    }



  }


  def scramble[T : ClassTag]( rdd : RDD[T], partitions : Int= 3) = {
    val wRandom = rdd.map((Math.random(), _))
    val unsorted = wRandom.sortByKey(true, partitions)
    unsorted.values
  }

}
