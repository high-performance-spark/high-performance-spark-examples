package com.highperformancespark.examples.goldilocks

import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
 * Computes Rank statistics and distinct values on data that has been mapped to
 * Value, columnIndex, count triples.
 *These methods aren't currently used in the Aggregation plugin,
 *  because the group by key field is required
 * @param valPairs
 * @param colIndexList
 */
class RankStats(valPairs : RDD[((Double, Int), Long )],
                val colIndexList : List[Int]) {
  private var reduced : RDD[((Double, Int), Long )]  = _
  private var sorted  : RDD[((Double, Int), Long )]  = _
  val n = colIndexList.last+1

  def getDistinctByColumn() = {
    reduced  = valPairs.reduceByKey((a, b) => a + b)
    val countsByPartition = getDistinctForeachPart()
    val distinct = getTotalDistinct(countsByPartition)
    distinct
  }

  def getMedianAndDistinct() = {
    //toDo: use range partitioner to sort this
    reduced = valPairs.reduceByKey((a, b) => a + b)
    sorted = reduced.sortByKey()
    val countAndSumByPart = getDistinctAndTotalForeachPart()
    val (distinct, total) = getDistinctAndTotal(countAndSumByPart)
    val half : Long = total.head / 2
    val sumsByPart = countAndSumByPart.sortBy(x => x._1)map{ case(index, (counts, sums ) ) => (index, sums)}
    val mapOfLocations = getLocationsOfRanksWithinEachPart(sumsByPart, List(half))
    val medians = findElementsIteratively(mapOfLocations).groupByKey().collectAsMap()
    (medians, distinct)
  }

  /**
   * Median and distinct step 1:
   * Gets the number of distinct values and the total values by colIndex and group on each
   * partition. Doing this in two steps should prevent large shuffles across the network.
   *
   * Returns with a triple for each partition with:
   * 1. The index of the partition
   * 2. A array of (colIndex) -> distinct values
   * 3. A array of (colIndex) -> sum of Values
   *    (this is used in the 'getLocationsOfRanksWithinEachPart' method to calculate the medians)
   */
  protected def getDistinctAndTotalForeachPart() = {
    val zero = Array.fill[Long](n)(0)
    sorted.mapPartitionsWithIndex((index : Int, it : Iterator[((Double, Int), Long)]) => {
      val (countArray, sumArray) = PartitionProcessingUtil.totalAndDistinct(it, zero)
      Iterator((index, (countArray, sumArray)))
    }).collect()
  }

  /**
   * Median and distinct step 2.
   * Given the per-partition distinct value and total counts gives us the total number of
   * distinct values and the total number of values in the data set.
   * Note: Right now, I am allowing for the columns to have different numbers or
   * values, say because some of the cells had bad data, so the totals are for
   * each column index. In the method that calculates medians, i assume that they are the same.
   *
   * Note: To just find the median, this step isn't really needed
   *
   * @param totalForEachPart result of the getDistinctForeachPart method
   *
   * @return A tupple of:
   *         1. a array of (index) -> distinctValues
   *         2. a array of (index) -> total values
   */
  protected def getDistinctAndTotal(totalForEachPart :
                          Array[(Int, (Array[Long], Array[Long]))] ) = {
    val runningCounts = Array.fill[Long](n)(0L)
    val runningSums = Array.fill[Long](n)(0L)
    var i = 0
    while (i < totalForEachPart.length){
      val  (index, (countAr , sumAr)) = totalForEachPart(i)
      countAr.zipWithIndex.foreach{
        case (count, colIndex) => runningCounts(colIndex) += count }
      sumAr.zipWithIndex.foreach{
        case (sum, colIndex) => runningSums(colIndex) += sum }
      i +=1
    }
    (runningCounts, runningSums)
  }


  /**
   * Median and distinct step 3:
   * @param partitionMap- the result of the previous method
   * @return and Array, locations where locations(i) = (i, list of each (colIndex, Value)
   *         in that partition value pairs that correspond to one of the target rank statistics for that col
   */
  def getLocationsOfRanksWithinEachPart(
                                         partitionMap : Array[(Int, Array[Long])],
                                         targetRanks : List[Long ]) : Array[(Int, List[(Int, Long)])]  = {
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
   * Median and distinct step 4:
   *Returns an iterator of columnIndex,
   * value pairs which correspond only to the values at which are
   *         rank statistics.
   */
  def findElementsIteratively(locations : Array[(Int, List[(Int, Long)])]) = {
    sorted.mapPartitionsWithIndex((index : Int, it : Iterator[((Double, Int), Long)]) => {
      val targetsInThisPart = locations(index)._2
      val len = targetsInThisPart.length
      if(len >0 ) {
        val newIt = PartitionProcessingUtil.getValuesFromRanks(it, targetsInThisPart)
        newIt}
      else Iterator.empty
    } )
  }

  //Stuff used just to find distinct values bellow this

  /**
   * @return an array of each partition and three values
   *         1. An array of longs, which is the count for each column of the values on that partition
   *         2. The first value of that partition
   *         3. the last value on that partition
   */
  def getDistinctForeachPart() :
  Array[ Array[Long]] = {
    val zero = Array.fill[Long](n)(0)
    reduced.mapPartitionsWithIndex((index : Int, it : Iterator[((Double, Int), Long)]) => {

      //toDo: Would it be better to keep as iterator and loop?
      val keyPair : Array[Long] = it.aggregate(zero)(
        (a , v : ((Double ,Int), Long)) => {
          val ((value, colIndex) , count) = v
          //don't add the count, just as one, because we are looking for the distinct values
          a(colIndex) += 1
          a},
        (a : Array[Long], b : Array[Long]) => {
          require(a.length == b.length)
          a.zip(b).map{ case(aVal, bVal) => aVal + bVal}
        })
      Iterator( keyPair)
    }).collect()
  }

  def getTotalDistinct(totalForEachPart :
                       (Array[Array[Long]]) ) = {

    val runningTotal = totalForEachPart(0)
    var i = 1
    while (i < totalForEachPart.length){
      val  countAr = totalForEachPart(i)
      countAr.zipWithIndex.foreach{
        case (count, colIndex) => runningTotal(colIndex) += count }

      i +=1
    }
    runningTotal
  }

}

object PartitionProcessingUtil extends Serializable {

  def totalAndDistinct(it : Iterator[((Double, Int), Long)], zero : Array[Long] ) : (Array[Long], Array[Long]) = {
    //   val zero1 = Array.fill[Long](n)(0L)
    val keyPair : (Array[Long], Array[Long]) = it.aggregate((Array.fill[Long](zero.length)(0L), Array.fill[Long](zero.length)(0L)))(
      (acc : (Array[Long], Array[Long])  , v : ((Double ,Int), Long)) => {
        val (a, sumAr ) = acc
        val ((value, colIndex) , count) = v
        a(colIndex) = a(colIndex) + 1
        sumAr(colIndex) = sumAr(colIndex) + count
        (a, sumAr)},
      (a , b ) => {
        val (aCounts, aSums) = a
        val (bCounts, bSums) = b
        require(aCounts.length == bCounts.length)
        require(aSums.length == bSums.length)
        val nextCounts = aCounts.zip(bCounts).map{ case(aVal, bVal) => aVal + bVal}
        val nextSums = aSums.zip(bSums).map{case( aVal, bVal) => aVal + bVal}
        (nextCounts, nextSums)
      })
    keyPair
  }
  /**
   * @param it iterator from the map partitions step in the find elements iteratively step
   * @param targetsInThisPart the results of the 'getLocationsOfRanksWithinEachPart' method
   * @return The nth value (or values, if the target ranks list is longer than one)
   *         for each column, if that nth value is in this partition.
   */
  def getValuesFromRanks(it: Iterator[((Double, Int), Long)], targetsInThisPart: List[(Int, Long)]): Iterator[(Int, Double)] = {
    val partMap = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
    val keysInThisPart = targetsInThisPart.map(_._1).distinct
    val runningTotals: mutable.HashMap[Int, Long] = new mutable.HashMap()
    //toDo: refactor to not add 0L but user getOrElse update instead
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
}
