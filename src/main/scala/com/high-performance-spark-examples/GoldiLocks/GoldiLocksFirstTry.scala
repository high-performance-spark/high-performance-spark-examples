package com.highperformancespark.examples.goldilocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.Map;
import scala.collection.mutable.MutableList;

object GoldiLocksGroupByKey {
  //tag::groupByKey[]
  def findRankStatistics(
    pairRDD: RDD[(Int, Double)],
    ranks: List[Long]): Map[Int, List[Double]] = {
    pairRDD.groupByKey().mapValues(iter => {
      val ar = iter.toArray.sorted
      ranks.map(n => ar(n.toInt))
    }).collectAsMap()
  }
  //end::groupByKey[]
}

//tag::firstTry[]
object GoldiLocksFirstTry {

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
    * @param targetRanks the required ranks for every column
    *
    * @return map of (column index, list of target ranks)
    */
  def findQuantiles(dataFrame: DataFrame, targetRanks: List[Long]):
    Map[Int, Iterable[Double]] = {

    val valueColumnPairs: RDD[(Double, Int)] = getValueColumnIndexPairs(dataFrame)
    val sortedValueColumnPairs = valueColumnPairs.sortByKey()
    sortedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK)

    val numOfColumns = dataFrame.schema.length
    val partitionColumnsFreq = getColumnFreqPerPartition(sortedValueColumnPairs, numOfColumns)
    val ranksLocations  = getLocationsOfRanksWithinEachPart(targetRanks, partitionColumnsFreq, numOfColumns)
    val result = findElementsIteratively(sortedValueColumnPairs, ranksLocations)
    result.groupByKey().collectAsMap()
  }

  /**
   * Step 1. Map the rows to pairs of (value, column Index).
   *
   * For example:
   *
   * dataFrame:
   *     1.5, 1.25, 2.0
   *    5.25,  2.5, 1.5
   *
   * The output RDD will be:
   *    (1.5, 0) (1.25, 1) (2.0, 2) (5.25, 0) (2.5, 1) (1.5, 2)
   *
   * @param dataFrame dateframe of doubles
   *
   * @return RDD of pairs (value, column Index)
   */
  private def getValueColumnIndexPairs(dataFrame : DataFrame): RDD[(Double, Int)] = {
    dataFrame.flatMap(row => row.toSeq.zipWithIndex.map{ case (v, index) =>
      (v.toString.toDouble, index)})
  }

  /**
   * Step 2. Find the number of elements for each column in each partition.
   *
   * For Example:
   *
   * sortedValueColumnPairs:
   *    Partition 1: (1.5, 0) (1.25, 1) (2.0, 2) (5.25, 0)
   *    Partition 2: (7.5, 1) (9.5, 2)
   *
   * numOfColumns: 3
   *
   * The output will be:
   *    [(0, [2, 1, 1]), (1, [0, 1, 1])]
   *
   * @param sortedValueColumnPairs - sorted RDD of (value, column Index) pairs
   * @param numOfColumns the number of columns
   *
   * @return Array that contains (partition index, number of elements from every column on this partition)
   */
  private def getColumnFreqPerPartition(sortedValueColumnPairs: RDD[(Double, Int)], numOfColumns : Int):
    Array[(Int, Array[Long])] = {

    val zero = Array.fill[Long](numOfColumns)(0)

    def aggregateColumnFrequencies (partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]) = {
      val totalsPerPart : Array[Long] = valueColumnPairs.aggregate(zero)(
        (a : Array[Long], v : (Double ,Int)) => {
          val (value, colIndex) = v
          a(colIndex) = a(colIndex) + 1L
          a
        },
        (a : Array[Long], b : Array[Long]) => {
          require(a.length == b.length)
          a.zip(b).map{ case(aVal, bVal) => aVal + bVal}
        })

      Iterator((partitionIndex, totalsPerPart))
    }

    sortedValueColumnPairs.mapPartitionsWithIndex(aggregateColumnFrequencies).collect()
  }

  /**
   * Step 3: For each Partition determine the index of the elements that are desired rank statistics
   *
   * For Example:
   *    targetRanks: 5
   *    partitionColumnsFreq: [(0, [2, 3]), (1, [4, 1]), (2, [5, 2])]
   *    numOfColumns: 2
   *
   * The output will be:
   *    [(0, []), (1, [(0, 3)]), (2, [(1, 1)])]
   *
   * @param partitionColumnsFreq Array of (partition index, columns frequencies per this partition)
   *
   * @return  Array that contains (partition index, relevantIndexList where relevantIndexList(i) = the index
   *          of an element on this partition that matches one of the target ranks)
   */
  private def getLocationsOfRanksWithinEachPart(targetRanks : List[Long],
    partitionColumnsFreq : Array[(Int, Array[Long])], numOfColumns : Int) : Array[(Int, List[(Int, Long)])] = {

    val runningTotal = Array.fill[Long](numOfColumns)(0)

    partitionColumnsFreq.sortBy(_._1).map { case (partitionIndex, columnsFreq) =>
      val relevantIndexList = new MutableList[(Int, Long)]()

      columnsFreq.zipWithIndex.foreach{ case (colCount, colIndex)  => {
        val runningTotalCol = runningTotal(colIndex)
        val ranksHere: List[Long] = targetRanks.filter(rank => (runningTotalCol < rank && runningTotalCol + colCount >= rank))

        // for each of the rank statistics present add this column index and the index it will be at
        // on this partition (the rank - the running total)
        relevantIndexList ++= ranksHere.map(rank => (colIndex, rank - runningTotalCol))

        runningTotal(colIndex) += colCount
      }}

      (partitionIndex, relevantIndexList.toList)
    }
  }

  /**
    * Finds rank statistics elements using ranksLocations.
    *
    * @param sortedValueColumnPairs - sorted RDD of (value, colIndex) pairs
    * @param ranksLocations Array of (partition Index, list of (column index, rank index of this column at this partition))
    *
    * @return
    */
  private def findElementsIteratively(sortedValueColumnPairs : RDD[(Double, Int)],
                                      ranksLocations : Array[(Int, List[(Int, Long)])]): RDD[(Int, Double)] = {

    sortedValueColumnPairs.mapPartitionsWithIndex((partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]) => {
      val targetsInThisPart: List[(Int, Long)] = ranksLocations(partitionIndex)._2
      if (!targetsInThisPart.isEmpty) {
        val columnsRelativeIndex: Map[Int, List[Long]] = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
        val columnsInThisPart = targetsInThisPart.map(_._1).distinct

        val runningTotals : mutable.HashMap[Int, Long]=  new mutable.HashMap()
        runningTotals ++= columnsInThisPart.map(columnIndex => (columnIndex, 0L)).toMap

        val result : ArrayBuffer[(Int, Double)] = new scala.collection.mutable.ArrayBuffer()

        valueColumnPairs.foreach{ case(value, colIndex) => {
          if (runningTotals isDefinedAt colIndex) {
            val total = runningTotals(colIndex) + 1L
            runningTotals.update(colIndex, total)

            if (columnsRelativeIndex(colIndex).contains(total))
              result += ((colIndex, value))
          }
        }}

        result.toIterator
      }
      else {
        Iterator.empty
      }
    })
  }
}
//end::firstTry[]
