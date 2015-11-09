package com.highperformancespark.examples.goldilocks

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

//tag::hashMap[]
object GoldiLocksWithHashMap {

  def findQuantiles( dataFrame: DataFrame , targetRanks: List[Long] ) = {
    val valueIndexCountPairs: RDD[((Double, Int), Long)] = createHashMap(dataFrame)
    val n =  dataFrame.schema.length
    val sorted = valueIndexCountPairs.sortByKey()
    sorted.persist(StorageLevel.MEMORY_AND_DISK)
    val parts : Array[Partition] = sorted.partitions
    val map1 = getTotalsForeachPart(sorted, parts.length, n )
    val map2  = getLocationsOfRanksWithinEachPart(targetRanks, map1, n)
    val result = findElementsIteratively(sorted, map2)
    result.groupByKey().collectAsMap()
  }

  /**
   * Step 1. Map the rows to pairs of ((value, colIndex), count) where count is the number of times
   * that value and that pair appear on this partition
   * @param dataFrame of double columns to compute the rank statistics for
   * @return
   */
  def createHashMap(dataFrame : DataFrame) : RDD[((Double, Int), Long)] = {
    val map =  dataFrame.rdd.mapPartitions(it => {
      val hashMap = new mutable.HashMap[(Double, Int), Long]()
      it.foreach( row => {
        row.toSeq.zipWithIndex.foreach{ case (value, i) => {
          val v = value.toString.toDouble
          val key = (v, i)
          val count = hashMap.getOrElseUpdate(key, 0)
          hashMap.update(key, count + 1 )
        }}
      })
      val newM = hashMap.toArray
      newM.toIterator
    })
    map
  }

  /**
   * Step 2. Find the number of elements for each column in each partition
   * @param sorted  rdd of ((value, index), count) pairs which has already been sorted
   * @param numPartitions the number of partitions
   * @return an RDD the length of the number of partitions, where each row contains
   *         - the partition index
   *         - an array, totalsPerPart where totalsPerPart(i) = the number of elements in column
   *         i on this partition
   */
  private def getTotalsForeachPart(sorted: RDD[((Double, Int), Long)], numPartitions: Int, n : Int ) = {
    val zero = Array.fill[Long](n)(0)
    sorted.mapPartitionsWithIndex((index : Int, it : Iterator[((Double, Int), Long)]) => {
      val totalsPerPart : Array[Long] = it.aggregate(zero)(
        (a : Array[Long], v : ((Double ,Int), Long)) => {
          val ((value, colIndex) , count) = v
          a(colIndex) = a(colIndex) + count
          a},
        (a : Array[Long], b : Array[Long]) => {
          require(a.length == b.length)
          a.zip(b).map{ case(aVal, bVal) => aVal + bVal}
        })
      Iterator((index, totalsPerPart))
    }).collect()
  }

  /**
   * Step 3: For each Partition determine the index of the elements that are desired rank statistics
   * @param partitionMap- the result of the previous method
   * @return an Array, the length of the number of partitions where each row contains
   *         - the partition index
   *         - a list,  relevantIndexList where relevantIndexList(i) = the index of an element on this
   *         partition that matches one of the target ranks
   */
  private def getLocationsOfRanksWithinEachPart(targetRanks : List[Long],
    partitionMap : Array[(Int, Array[Long])], n : Int ) : Array[(Int, List[(Int, Long)])]  = {
    val runningTotal = Array.fill[Long](n)(0)
    partitionMap.sortBy(_._1).map { case (partitionIndex, totals)=> {
      val relevantIndexList = new  scala.collection.mutable.MutableList[(Int, Long)]()
      totals.zipWithIndex.foreach{ case (colCount, colIndex)  => {
        val runningTotalCol = runningTotal(colIndex)
        runningTotal(colIndex) += colCount
        val ranksHere = targetRanks.filter(rank =>
          (runningTotalCol <= rank && runningTotalCol + colCount >= rank)
        )
        ranksHere.foreach(rank => {
          relevantIndexList += ((colIndex, rank-runningTotalCol))
        })
      }} //end of mapping col counts
      (partitionIndex, relevantIndexList.toList)
    }}
  }

  /**
   * Step4: Using the results of the previous method, scan the  data and return the elements
   * which correspond to the rank statistics we are looking for in each column
   */
  private def findElementsIteratively(sorted : RDD[((Double, Int), Long)],
    locations : Array[(Int, List[(Int, Long)])]) = {
    sorted.mapPartitionsWithIndex((index : Int, it : Iterator[((Double, Int), Long)]) => {
      val targetsInThisPart = locations(index)._2
      val len = targetsInThisPart.length
      if(len >0 ) {
        val partMap = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
        val keysInThisPart = targetsInThisPart.map(_._1).distinct
        val runningTotals: mutable.HashMap[Int, Long] = new mutable.HashMap()
        keysInThisPart.foreach(key => runningTotals += ((key, 0L)))
        val newIt: ArrayBuffer[(Int, Double)] = new scala.collection.mutable.ArrayBuffer()
        it.foreach { case ((value, colIndex), count) => {
          if (keysInThisPart.contains(colIndex) ) {
            val total = runningTotals(colIndex)
            val ranksPresent =  partMap(colIndex).filter(v => (v <= count + total) && (v > total))
            ranksPresent.foreach(r => {
              newIt += ((colIndex, value))
            })
            runningTotals.update(colIndex, total + count)
          }
        }}
        newIt.toIterator
      }
      else Iterator.empty
    } )
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
    if(storageLevel!=StorageLevel.NONE){
      sorted.persist(storageLevel)
    }
    if(checkPoint){
      sorted.sparkContext.setCheckpointDir(directory)
      sorted.checkpoint()
    }
    val parts : Array[Partition] = sorted.partitions
    val map1 = getTotalsForeachPart(sorted, parts.length, n)
    val map2  = getLocationsOfRanksWithinEachPart(targetRanks, map1, n)
    val result = findElementsIteratively(sorted, map2)
    result.groupByKey().collectAsMap()
  }
}
//end::hashMap[]