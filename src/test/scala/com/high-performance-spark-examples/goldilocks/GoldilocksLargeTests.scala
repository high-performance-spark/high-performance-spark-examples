package com.highperformancespark.examples.goldilocks

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, DoubleType, StructField}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq

class GoldilocksLargeTests extends FunSuite with SharedSparkContext{


  def testGoldilocksImplementations(
    data : DataFrame, targetRanks : List[Long],
    expectedResult :  Map[Int, Iterable[Long]])= {
    val iterative = GoldilocksWhileLoop.findRankStatistics(data, targetRanks)
    val groupByKey = GoldilocksGroupByKey.findRankStatistics(data, targetRanks)
    val firstTry = GoldilocksFirstTry.findRankStatistics(data, targetRanks)
    val hashMap = GoldilocksWithHashMap.findRankStatistics(data, targetRanks)
    val secondarySort = GoldilocksSecondarySort.findRankStatistics(data, targetRanks)
    val secondarySortV2 = GoldilocksSecondarySortV2.findRankStatistics(data, targetRanks)

    expectedResult.foreach {
      case((i, ranks)) =>
        assert(iterative(i).equals(ranks),
        "The Iterative solution to goldilocks was incorrect for column " + i)
        assert(groupByKey(i).equals(ranks), "Group by key solution was incorrect")
        assert(firstTry(i).equals(ranks), "GoldilocksFirstTry incorrect for column " + i )
        assert(hashMap(i).equals(ranks), "GoldilocksWithhashMap incorrect for column " + i)
        assert(secondarySort(i).equals(ranks))
        assert(secondarySortV2(i).equals(ranks))

    }
  }

  test("Goldilocks on local data solution "){
        val sqlContext = new SQLContext(sc)
        val testRanks = List(3L, 8L)
        val (smallTestData, result) = DataCreationUtils.createLocalTestData(5, 10, testRanks)
        val schema = StructType(result.keys.toSeq.map(n => StructField("Column" + n.toString, DoubleType)))
        val smallTestDF : DataFrame  = sqlContext.createDataFrame(sc.makeRDD(smallTestData), schema)
        testGoldilocksImplementations(smallTestDF, testRanks, result)
    }
}

object DataCreationUtils {
  def createLocalTestData(numberCols : Int, numberOfRows : Int, targetRanks : List[Long]) = {
    val cols = Range(0,numberCols).toArray
    val scalers = cols.map(x => 1.0)
    val rowRange =  Range(0, numberOfRows)
    val columnArray: Array[IndexedSeq[Double]] = cols.map(
      columnIndex => {
        val columnValues = rowRange.map(x => (Math.random(), x)).sortBy(_._1).map(_._2 * scalers(columnIndex))
        columnValues
      })
    val rows = rowRange.map(
      rowIndex => {
        Row.fromSeq(cols.map( colI => columnArray(colI)(rowIndex)).toSeq)
      })


    val result: Map[Int, Iterable[Long]] = cols.map(i => {
      (i, targetRanks.map(r => Math.round((r-1)/scalers(i))))
    }).toMap

    (rows, result)
  }


  def createDistributedData(sc : SparkContext ,partitions: Int, elementsPerPartition : Int, numberOfColumns : Int ) = {
    val partitionsStart: RDD[Int] = sc.parallelize(
      Array.fill(partitions)(1))
    partitionsStart.repartition(partitions)

    var data: RDD[(Long, List[Int])] = partitionsStart.mapPartitionsWithIndex {
      case (partIndex, elements) =>
        val rows = Range(0, elementsPerPartition)
                   .map(x => (Math.random(), x))
                   .map {
                     case ((randomNumber, rowValue)) =>
                       (
                         randomNumber,
                         (partIndex * elementsPerPartition.toLong + rowValue, //index of element
                           List((rowValue + partIndex * elementsPerPartition))))
                   }
        rows.toIterator
    }.sortByKey().values


    Range(0, numberOfColumns).foreach(x => {
      val nextColumn: RDD[(Long, Int)] = partitionsStart.mapPartitionsWithIndex {
        case (partIndex, elements) =>
          val rows = Range(0, elementsPerPartition)
                     .map(x => (Math.random(), x))
                     .map {
                       case ((randomNumber, rowValue)) =>
                         (
                           randomNumber,
                           (partIndex * elementsPerPartition.toLong + rowValue, //index of element
                             rowValue + partIndex * elementsPerPartition))
                     }
          rows.toIterator
      }.sortByKey().values

      data = nextColumn.join(data).mapValues(x => x._1 :: x._2)
    })
    data
  }
}
