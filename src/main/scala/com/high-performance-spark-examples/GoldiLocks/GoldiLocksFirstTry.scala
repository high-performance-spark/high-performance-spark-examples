package com.highperformancespark.examples.goldilocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.MutableList
import scala.collection.{Map, mutable}

object GoldilocksGroupByKey {
  //tag::groupByKey[]
  def findRankStatistics(
    dataFrame: DataFrame ,
    ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    //Map to column index, value pairs
    val pairRDD: RDD[(Int, Double)] = mapToKeyValuePairs(dataFrame)

    val groupColumns: RDD[(Int, Iterable[Double])] = pairRDD.groupByKey()
    groupColumns.mapValues(
      iter => {
        //convert to an array and sort
        val sortedIter = iter.toArray.sorted

        sortedIter.toIterable.zipWithIndex.flatMap({
        case (colValue, index) =>
          if (ranks.contains(index + 1))
            Iterator(colValue)
          else
            Iterator.empty
      })
    }).collectAsMap()
  }

  def findRankStatistics(
    pairRDD: RDD[(Int, Double)],
    ranks: List[Long]): Map[Int, Iterable[Double]] = {
    assert(ranks.forall(_ > 0))
    pairRDD.groupByKey().mapValues(iter => {
      val sortedIter  = iter.toArray.sorted
      sortedIter.zipWithIndex.flatMap(
        {
        case (colValue, index) =>
          if (ranks.contains(index + 1))
            Iterator(colValue)  //this is one of the desired rank statistics
          else
            Iterator.empty
        }
      ).toIterable //convert to more generic iterable type to match out spec
    }).collectAsMap()
  }
  //end::groupByKey[]


  //tag::toKeyValPairs[]
  def mapToKeyValuePairs(dataFrame: DataFrame): RDD[(Int, Double)] = {
    val rowLength = dataFrame.schema.length
    dataFrame.rdd.flatMap(
      row => Range(0, rowLength).map(i => (i, row.getDouble(i)))
    )
  }
  //end::toKeyValPairs[]
}


object GoldilocksWhileLoop{

  //tag::rankstatsLoop[]
  def findRankStatistics(
    dataFrame: DataFrame,
    ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    val numberOfColumns = dataFrame.schema.length
    var i = 0
    var  result = Map[Int, Iterable[Double]]()

    while(i < numberOfColumns){
      val col = dataFrame.rdd.map(row => row.getDouble(i))
      val sortedCol : RDD[(Double, Long)] = col.sortBy(v => v).zipWithIndex()
      val ranksOnly = sortedCol.filter{
        case (colValue, index) =>  ranks.contains(index + 1) //rank statistics are indexed from one. e.g. first element is 0
      }.keys
      val list = ranksOnly.collect()
       result += (i -> list)
       i+=1
    }
    result
  }
  //end::rankstatsLoop[]
}

//tag::firstTry[]
object GoldilocksFirstTry {

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
  def findRankStatistics(dataFrame: DataFrame, targetRanks: List[Long]):
    Map[Int, Iterable[Double]] = {

    val valueColumnPairs: RDD[(Double, Int)] = getValueColumnPairs(dataFrame)
    val sortedValueColumnPairs = valueColumnPairs.sortByKey()
    sortedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK)

    val numOfColumns = dataFrame.schema.length
    val partitionColumnsFreq =
      getColumnsFreqPerPartition(sortedValueColumnPairs, numOfColumns)
    val ranksLocations =
      getRanksLocationsWithinEachPart(targetRanks, partitionColumnsFreq, numOfColumns)

    val targetRanksValues = findTargetRanksIteratively(sortedValueColumnPairs, ranksLocations)
    targetRanksValues.groupByKey().collectAsMap()
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
  private def getValueColumnPairs(dataFrame : DataFrame): RDD[(Double, Int)] = {
    dataFrame.rdd.flatMap{row: Row => row.toSeq.zipWithIndex.map{ case (v, index) =>
      (v.toString.toDouble, index)}}
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
  private def getColumnsFreqPerPartition(sortedValueColumnPairs: RDD[(Double, Int)], numOfColumns : Int):
    Array[(Int, Array[Long])] = {

    val zero = Array.fill[Long](numOfColumns)(0)

    def aggregateColumnFrequencies (partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]) = {
      val columnsFreq : Array[Long] = valueColumnPairs.aggregate(zero)(
        (a : Array[Long], v : (Double ,Int)) => {
          val (value, colIndex) = v
          a(colIndex) = a(colIndex) + 1L
          a
        },
        (a : Array[Long], b : Array[Long]) => {
          a.zip(b).map{ case(aVal, bVal) => aVal + bVal}
        })

      Iterator((partitionIndex, columnsFreq))
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
  private def getRanksLocationsWithinEachPart(targetRanks : List[Long],
                                              partitionColumnsFreq : Array[(Int, Array[Long])],
                                              numOfColumns : Int) : Array[(Int, List[(Int, Long)])] = {

    val runningTotal = Array.fill[Long](numOfColumns)(0)

    partitionColumnsFreq.sortBy(_._1).map { case (partitionIndex, columnsFreq) =>
      val relevantIndexList = new MutableList[(Int, Long)]()

      columnsFreq.zipWithIndex.foreach{ case (colCount, colIndex)  =>
        val runningTotalCol = runningTotal(colIndex)
        val ranksHere: List[Long] = targetRanks.filter(rank =>
          runningTotalCol < rank && runningTotalCol + colCount >= rank)

        // for each of the rank statistics present add this column index and the index it will be at
        // on this partition (the rank - the running total)
        relevantIndexList ++= ranksHere.map(rank => (colIndex, rank - runningTotalCol))

        runningTotal(colIndex) += colCount
      }

      (partitionIndex, relevantIndexList.toList)
    }
  }

  /**
    * Finds rank statistics elements using ranksLocations.
    *
    * @param sortedValueColumnPairs - sorted RDD of (value, colIndex) pairs
    * @param ranksLocations Array of (partition Index, list of (column index, rank index of this column at this partition))
    *
    * @return returns RDD of the target ranks (column index, value)
    */
  private def findTargetRanksIteratively(sortedValueColumnPairs : RDD[(Double, Int)],
                                      ranksLocations : Array[(Int, List[(Int, Long)])]): RDD[(Int, Double)] = {

    sortedValueColumnPairs.mapPartitionsWithIndex((partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]) => {
      val targetsInThisPart: List[(Int, Long)] = ranksLocations(partitionIndex)._2
      if (targetsInThisPart.nonEmpty) {
        val columnsRelativeIndex: Map[Int, List[Long]] = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
        val columnsInThisPart = targetsInThisPart.map(_._1).distinct

        val runningTotals : mutable.HashMap[Int, Long]=  new mutable.HashMap()
        runningTotals ++= columnsInThisPart.map(columnIndex => (columnIndex, 0L)).toMap

        //filter this iterator, so that it contains only those (value, columnIndex) that are the ranks statistics on this partition
        // I.e. Keep track of the number of elements we have seen for each columnIndex using the
        // running total hashMap. Keep those pairs for which value is the nth element for that columnIndex that appears on this partition
        // and the map contains (columnIndex, n).
        valueColumnPairs.filter{
          case(value, colIndex) =>
            //rely on lazy evaluation. If we have already seen this column index, then evalute this
            // block in which we increment the running totals and return if this element's count appears in the map.
            lazy val thisPairIsTheRankStatistic: Boolean = {
              val total = runningTotals(colIndex) + 1L
              runningTotals.update(colIndex, total)
              columnsRelativeIndex(colIndex).contains(total)
            }
             (runningTotals contains colIndex) && thisPairIsTheRankStatistic
        }.map(_.swap)
      }
      else {
        Iterator.empty
      }
    })
  }
}
//end::firstTry[]
