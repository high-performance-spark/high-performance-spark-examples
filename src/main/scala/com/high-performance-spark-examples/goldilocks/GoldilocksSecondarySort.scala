package com.highperformancespark.examples.goldilocks

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

class ColumnIndexPartition(override val numPartitions: Int)
  extends Partitioner {
  require(numPartitions >= 0, s"Number of partitions " +
    s"($numPartitions) cannot be negative.")

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(Int, Double)]
    Math.abs(k._1) % numPartitions //hashcode of column index
  }
}

object GoldilocksSecondarySort {
  /**
    * Find nth target rank for every column.
    *
    * For example:
    *
    * dataframe:
    *   (0.0, 4.5, 7.7, 5.0)
    *   (1.0, 5.5, 6.7, 6.0)
    *   (2.0, 5.5, 1.5, 7.0)
    *   (3.0, 5.5, 0.5, 7.0)
    *   (4.0, 5.5, 0.5, 8.0)
    *
    * targetRanks:
    *   1, 3
    *
    * The output will be:
    *   0 -> (0.0, 2.0)
    *   1 -> (4.5, 5.5)
    *   2 -> (7.7, 1.5)
    *   3 -> (5.0, 7.0)
    *
    * @param dataFrame dataframe of doubles
    * @param ranks the required ranks for every column
    * @param partitions - the number of partitions to use when sorting
    * @return map of (column index, list of target ranks)
    */
  def findRankStatistics(
    dataFrame: DataFrame,
    ranks: List[Long], partitions: Int = 2): Map[Int, Iterable[Double]] = {
    val pairs = GoldilocksGroupByKey.mapToKeyValuePairs(dataFrame)
    findRankStatistics(pairs, ranks, partitions)
  }

  /**
    *
    * Helper Function for Secondary sort implementation of the goldilocks problem.
    * 0. Map to ((columnIndex, cellValue), 1) tripples.
    * 1. Define a custom partitioner which partitions according to the first half of the key.
    *  (column Index)
    * 1. uses repartitionAndSortWithinPartitions with the custom partitioner. This will partition
    * according to column index and then sort by column index and value.
    * 2. mapPartitions on each partition which is sorted. Filter for correct rank stats in one pass.
    * 3. collect result
    * 4. group result so that each key has an iterator of elements.
    * @param pairRDD - an RDD of (columnIndex , cellValue) pairs
    * @param targetRanks the rank statistics to find for every column.
    */
  def findRankStatistics(pairRDD: RDD[(Int, Double)],
    targetRanks: List[Long], partitions: Int) = {
    val partitioner = new ColumnIndexPartition(partitions)
    val sorted = pairRDD.map((_, 1)).repartitionAndSortWithinPartitions(partitioner)
    val filterForTargetIndex = sorted.mapPartitions(iter => {
      var currentColumnIndex = -1
      var runningTotal = 0
      iter.flatMap({
        case (((colIndex, value), _)) =>
          if (colIndex != currentColumnIndex) {
            currentColumnIndex = colIndex //reset to the new column index
            runningTotal = 1
          } else {
            runningTotal += 1
          }
          //if the running total corresponds to one of the rank statistics.
          //keep this ((colIndex, value)) pair.
          if (targetRanks.contains(runningTotal)) {
            Iterator((colIndex, value))

          } else {
            Iterator.empty
          }
      })
    }, preservesPartitioning = true)
    groupSorted(filterForTargetIndex.collect())
  }

  /**
    * Given an array of (columnIndex, value) pairs that are already sorted.
    * Groups the pairs with the same column index, creating an iterator of values.
    */
  private def groupSorted(
    it: Array[(Int, Double)]): Map[Int, Iterable[Double]] = {
    val res = List[(Int, ArrayBuffer[Double])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil =>
        val (firstKey, value) = next
        List((firstKey, ArrayBuffer(value)))

      case head :: rest =>
        val (curKey, valueBuf) = head
        val (firstKey, value) = next
        if (!firstKey.equals(curKey)) {
          (firstKey, ArrayBuffer(value)) :: list
        } else {
          valueBuf.append(value)
          list
        }

    }).map { case (key, buf) => (key, buf.toIterable) }.toMap
  }
}

object GoldilocksSecondarySortV2{
  //tag::goldilocksSecondarySort[]
  def findRankStatistics(dataFrame: DataFrame,
  ranks: List[Long], partitions : Int = 2) : Map[Int, Iterable[Double]] = {
    val pairRDD = GoldilocksGroupByKey.mapToKeyValuePairs(dataFrame)
    val partitioner = new ColumnIndexPartition(partitions)
    val sorted = pairRDD.map((_, 1)).repartitionAndSortWithinPartitions(partitioner)
    val filterForTargetIndex= sorted.keys.mapPartitions(iter => {
        filterAndGroupRanks(iter, ranks)
    }, true)
    filterForTargetIndex.collectAsMap()
  }

  //end::goldilocksSecondarySort[]

  /**
    * Precondintion: Iterator must be sorted by (columnIndex, value). Groups by column index and filters
    * the values so that only those that correspond to the desired rank statistics are included.
    * @param it
    * @param targetRanks
    * @return
    */
  //tag::groupSortedGoldilocks[]
  def filterAndGroupRanks(
    it: Iterator[(Int, Double)], targetRanks : List[Long]): Iterator[(Int, Iterable[Double])] = {
    val res = List[(Int, Long, ArrayBuffer[Double])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil =>
        val (firstKey, value) = next
        val runningTotal = 1L
        val ranksSoFar: ArrayBuffer[Double] =
          if(targetRanks.contains(runningTotal)) ArrayBuffer(value) else ArrayBuffer[Double]()
        List((firstKey, runningTotal, ranksSoFar))

      case head :: rest =>
        val (curKey, runningTotal, valueBuf) = head
        val (firstKey, value) = next

        if (!firstKey.equals(curKey) ) {
          val resetRunningTotal = 1L
          val nextBuf = if(targetRanks.contains(resetRunningTotal))
            ArrayBuffer[Double](value) else ArrayBuffer[Double]()
          (firstKey, resetRunningTotal, nextBuf) :: list
        } else {
          val newRunningTotal = runningTotal + 1
          if(targetRanks.contains(newRunningTotal)){
            valueBuf.append(value)
          }
          (curKey, newRunningTotal, valueBuf) :: rest
        }

    }).map { case (key, total, buf) => (key, buf.toIterable) }.iterator
  }
  //end::groupSortedGoldilocks[]
}







