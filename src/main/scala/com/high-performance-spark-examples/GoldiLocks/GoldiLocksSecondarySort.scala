package com.highperformancespark.examples.goldilocks

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer



object GoldilocksSecondarySort {

  def findRankStatistics(
    dataFrame: DataFrame,
    ranks: List[Long], partitions : Int = 2) : Map[Int, Iterable[Double]] = {
    val pairs = GoldilocksGroupByKey.mapToKeyValuePairs(dataFrame)
    findRankStatistics(pairs, ranks, partitions)
  }

  //tag::goldilocksSecondarySort[]
  def findRankStatistics(pairRDD: RDD[(Int, Double)],
    targetRanks: List[Long], partitions : Int ) = {
    val partitioner = new HashPartitioner(partitions)
    val sorted = pairRDD.repartitionAndSortWithinPartitions(partitioner)
    val filterForTargetIndex: RDD[(Int, Iterable[Double])] = sorted.mapPartitions(iter => {
      var currentIndex = -1
      var elementsPerIndex = 0
     val filtered =  iter.filter {
        case (colIndex, value) =>
          if (colIndex != currentIndex) {
            currentIndex = 1
            elementsPerIndex = 1
          } else {
            elementsPerIndex += 1
          }
          targetRanks.contains(elementsPerIndex)
      }
      groupSorted(filtered)

    }, preservesPartitioning = true)

    filterForTargetIndex.collectAsMap()
  }

  //end::goldilocksSecondarySort[]

  //tag::groupSortedGoldilocks[]
  def groupSorted(
    it: Iterator[(Int, Double)]): Iterator[(Int, Iterable[Double])] = {
    val res = List[(Int, ArrayBuffer[Double])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil =>
        val (firstKey, value) = next
        List((firstKey, ArrayBuffer( value)))

      case head :: rest =>
        val (curKey, valueBuf) = head
        val (firstKey, value) = next
        if (!firstKey.equals(curKey) ) {
          (firstKey, ArrayBuffer( value)) :: list
        } else {
          valueBuf.append( value)
          list
        }

    }).map { case (key, buf) => (key, buf.toIterable) }.iterator
  }
  //tag::groupSortedGoldilocks[]
}

