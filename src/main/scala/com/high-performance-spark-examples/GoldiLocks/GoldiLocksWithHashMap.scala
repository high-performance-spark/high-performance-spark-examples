package com.highperformancespark.examples.goldilocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.MutableList

//tag::hashMap[]
object GoldiLocksWithHashMap {

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
    
    val aggregatedValueColumnPairs: RDD[((Double, Int), Long)] = getAggregatedValueColumnPairs(dataFrame)
    val sortedAggregatedValueColumnPairs = aggregatedValueColumnPairs.sortByKey()
    sortedAggregatedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK)

    val numOfColumns =  dataFrame.schema.length
    val partitionColumnsFreq = getColumnsFreqPerPartition(sortedAggregatedValueColumnPairs, numOfColumns)
    val ranksLocations  = getRanksLocationsWithinEachPart(targetRanks, partitionColumnsFreq, numOfColumns)

    val targetRanksValues = findTargetRanksIteratively(sortedAggregatedValueColumnPairs, ranksLocations)
    targetRanksValues.groupByKey().collectAsMap()
  }

  /**
   * Step 1. Map the rows to pairs of ((value, colIndex), count) where count is the number of times
   * that value and that pair appear on this partition
   *
   * For example:
   *
   * dataFrame:
   *     1.5, 1.25, 2.0
   *     1.5,  2.5, 2.0
   *
   * The output RDD will be:
   *    ((1.5, 0), 2) ((1.25, 1), 1) ((2.5, 1), 1) ((2.0, 2), 2)
   *
   * @param dataFrame of double columns to compute the rank statistics for
   *
   * @return returns RDD of ((value, column index), count)
   */
  def getAggregatedValueColumnPairs(dataFrame : DataFrame) : RDD[((Double, Int), Long)] = {
    val aggregatedValueColumnRDD =  dataFrame.rdd.mapPartitions(rows => {
      val valueColumnMap = new mutable.HashMap[(Double, Int), Long]()
      rows.foreach(row => {
        row.toSeq.zipWithIndex.foreach{ case (value, columnIndex) =>
          val key = (value.toString.toDouble, columnIndex)
          val count = valueColumnMap.getOrElseUpdate(key, 0)
          valueColumnMap.update(key, count + 1)
        }
      })

      valueColumnMap.toIterator
    })

    aggregatedValueColumnRDD
  }

  /**
    * Step 2. Find the number of elements for each column in each partition.
    *
    * For Example:
    *
    * sortedValueColumnPairs:
    *    Partition 1: ((1.5, 0), 2) ((2.0, 0), 1)
    *    Partition 2: ((4.0, 0), 3) ((3.0, 1), 1)
    *
    * numOfColumns: 3
    *
    * The output will be:
    *    [(0, [3, 0]), (1, [3, 1])]
    *
    * @param sortedAggregatedValueColumnPairs - sortedAggregatedValueColumnPairs RDD of ((value, column index), count)
    * @param numOfColumns the number of columns
    *
    * @return Array that contains (partition index, number of elements from every column on this partition)
    */
  private def getColumnsFreqPerPartition(sortedAggregatedValueColumnPairs: RDD[((Double, Int), Long)],
                                        numOfColumns : Int): Array[(Int, Array[Long])] = {

    val zero = Array.fill[Long](numOfColumns)(0)
    def aggregateColumnFrequencies(partitionIndex : Int, pairs : Iterator[((Double, Int), Long)]) = {
      val columnsFreq : Array[Long] = pairs.aggregate(zero)(
        (a : Array[Long], v : ((Double,Int), Long)) => {
          val ((value, colIndex), count) = v
          a(colIndex) = a(colIndex) + count
          a},
        (a : Array[Long], b : Array[Long]) => {
          a.zip(b).map{ case(aVal, bVal) => aVal + bVal}
        })

      Iterator((partitionIndex, columnsFreq))
    }

    sortedAggregatedValueColumnPairs.mapPartitionsWithIndex(aggregateColumnFrequencies).collect()
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
                                              numOfColumns : Int) : Array[(Int, List[(Int, Long)])]  = {

    val runningTotal = Array.fill[Long](numOfColumns)(0)

    partitionColumnsFreq.sortBy(_._1).map { case (partitionIndex, columnsFreq)=> {
      val relevantIndexList = new MutableList[(Int, Long)]()

      columnsFreq.zipWithIndex.foreach{ case (colCount, colIndex)  => {
        val runningTotalCol = runningTotal(colIndex)

        val ranksHere: List[Long] = targetRanks.filter(rank =>
          (runningTotalCol < rank && runningTotalCol + colCount >= rank))
        relevantIndexList ++= ranksHere.map(rank => (colIndex, rank - runningTotalCol))

        runningTotal(colIndex) += colCount
      }}

      (partitionIndex, relevantIndexList.toList)
    }}
  }

  /**
    * Finds rank statistics elements using ranksLocations.
    *
    * @param sortedAggregatedValueColumnPairs - sorted RDD of (value, colIndex) pairs
    * @param ranksLocations Array of (partition Index, list of (column index, rank index of this column at this partition))
    *
    * @return returns RDD of the target ranks (column index, value)
    */
  private def findTargetRanksIteratively(sortedAggregatedValueColumnPairs : RDD[((Double, Int), Long)],
                                         ranksLocations : Array[(Int, List[(Int, Long)])]): RDD[(Int, Double)] = {

    sortedAggregatedValueColumnPairs.mapPartitionsWithIndex((partitionIndex : Int,
      aggregatedValueColumnPairs : Iterator[((Double, Int), Long)]) => {

      val targetsInThisPart = ranksLocations(partitionIndex)._2
      if (targetsInThisPart.nonEmpty) {
        val columnsRelativeIndex = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
        val columnsInThisPart = targetsInThisPart.map(_._1).distinct

        val runningTotals : mutable.HashMap[Int, Long]=  new mutable.HashMap()
        runningTotals ++= columnsInThisPart.map(columnIndex => (columnIndex, 0L)).toMap

        val result: ArrayBuffer[(Int, Double)] = new scala.collection.mutable.ArrayBuffer()

        aggregatedValueColumnPairs.foreach { case ((value, colIndex), count) => {
          if (columnsInThisPart contains colIndex) {
            val total = runningTotals(colIndex)

            val ranksPresent =  columnsRelativeIndex(colIndex)
              .filter(index => (index <= count + total) && (index > total))
            ranksPresent.foreach(r => result += ((colIndex, value)))

            runningTotals.update(colIndex, total + count)
          }
        }}

        result.toIterator
      }
      else Iterator.empty
    })
  }

  /**
   * We will want to use this in some chapter where we talk about check pointing
   * @param valPairs
   * @param colIndexList
   * @param targetRanks
   * @param storageLevel
   * @param checkPoint
   * @param directory
   * @return
   */
  def findQuantilesWithCustomStorage(valPairs: RDD[((Double, Int), Long)],
    colIndexList: List[Int],
    targetRanks: List[Long],
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    checkPoint : Boolean, directory : String = "") = {

    val n = colIndexList.last+1
    val sorted  = valPairs.sortByKey()
    if (storageLevel != StorageLevel.NONE)
      sorted.persist(storageLevel)

    if (checkPoint) {
      sorted.sparkContext.setCheckpointDir(directory)
      sorted.checkpoint()
    }

    val partitionColumnsFreq = getColumnsFreqPerPartition(sorted, n)
    val ranksLocations  = getRanksLocationsWithinEachPart(targetRanks, partitionColumnsFreq, n)
    val targetRanksValues = findTargetRanksIteratively(sorted, ranksLocations)
    targetRanksValues.groupByKey().collectAsMap()
  }
}
//end::hashMap[]