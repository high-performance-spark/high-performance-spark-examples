package com.highperformancespark.examples.goldilocks

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object GoldiLocksGroupByKey {
  //tag::groupByKey[]
  def findRankStatistics(
    pairRDD: RDD[(Int, Double)],
    ranks: List[Long]): scala.collection.Map[Int, List[Double]] = {
    pairRDD.groupByKey().mapValues(iter => {
      val ar = iter.toArray.sorted
      ranks.map(n => ar(n.toInt))
    }).collectAsMap()
  }
  //end::groupByKey[]
}

//tag::firstTry[]
object GoldiLocksFirstTry {

  def findQuantiles( dataFrame: DataFrame, targetRanks: List[Long] ) = {
    val n = dataFrame.schema.length
    val  valPairs: RDD[(Double, Int)] = getPairs(dataFrame)
    val sorted = valPairs.sortByKey()
    sorted.persist(StorageLevel.MEMORY_AND_DISK)
    val parts : Array[Partition] = sorted.partitions
    val map1 = getTotalsForeachPart(sorted, parts.length, n )
    val map2  = getLocationsOfRanksWithinEachPart(targetRanks, map1, n)
    val result = findElementsIteratively(sorted, map2)
    result.groupByKey().collectAsMap()
  }

  /**
   * Step 1. Map the rows to pairs of (value, colIndex)
   * @param dataFrame of double columns to compute the rank satistics for
   * @return
   */
  private def getPairs(dataFrame : DataFrame ): RDD[(Double, Int )] ={
    dataFrame.flatMap( row => row.toSeq.zipWithIndex.map{ case (v, index ) =>
      (v.toString.toDouble, index )})
  }

  /**
   * Step 2. Find the number of elements for each column in each partition
   * @param sorted - the sorted (value, colIndex) pairs
   * @param numPartitions
   * @param n the number of columns
   * @return an RDD the length of the number of partitions, where each row contains
   *         - the partition index
   *         - an array, totalsPerPart where totalsPerPart(i) = the number of elements in column
   *         i on this partition
   */
  private def getTotalsForeachPart(sorted: RDD[(Double, Int)], numPartitions: Int, n : Int ) = {
    val zero = Array.fill[Long](n)(0)
    sorted.mapPartitionsWithIndex((partitionIndex : Int, it : Iterator[(Double, Int)]) => {
      val totalsPerPart : Array[Long] = it.aggregate(zero)(
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
    partitionMap.sortBy(_._1).map { case (partitionIndex, totals)=>
      val relevantIndexList = new  scala.collection.mutable.MutableList[(Int, Long)]()
      totals.zipWithIndex.foreach{ case (colCount, colIndex)  => {
        val runningTotalCol = runningTotal(colIndex)
        runningTotal(colIndex) += colCount
        val ranksHere = targetRanks.filter(rank =>
          runningTotalCol <= rank && runningTotalCol + colCount >= rank
        )
        //for each of the rank statistics present add this column index and the index it will be
        //at on this partition (the rank - the running total)
        ranksHere.foreach(rank => {
          relevantIndexList += ((colIndex, rank-runningTotalCol))
        })
      }}
      (partitionIndex, relevantIndexList.toList)
    }
  }

  /**
   * Step4: Using the results of the previous method, scan the  data and return the elements
   * which correspond to the rank statistics we are looking for in each column
   */
  private def findElementsIteratively(sorted : RDD[(Double, Int)], locations : Array[(Int, List[(Int, Long)])]) = {
    sorted.mapPartitionsWithIndex((index : Int, it : Iterator[(Double, Int)]) => {
      val targetsInThisPart = locations(index)._2
      val len = targetsInThisPart.length
      if (len > 0) {
        val partMap = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
        val keysInThisPart = targetsInThisPart.map(_._1).distinct
        val runningTotals : mutable.HashMap[Int, Long]=  new mutable.HashMap()
        keysInThisPart.foreach(key => runningTotals+=((key, 0L)))
        val newIt : ArrayBuffer[(Int, Double)] = new scala.collection.mutable.ArrayBuffer()
        it.foreach{ case( value, colIndex) => {
          if(runningTotals isDefinedAt colIndex){
            val total = runningTotals(colIndex) + 1L
            runningTotals.update(colIndex, total)
            if(partMap(colIndex).contains(total)){
              newIt += ((colIndex,value ))
            }
          }
        }}
        newIt.toIterator
      }
      else {
        Iterator.empty
      }
    })
  }
}
//end::firstTry[]
