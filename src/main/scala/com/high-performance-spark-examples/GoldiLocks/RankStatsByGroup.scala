package com.highperformancespark.examples.goldilocks

import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

class RankStatsByGroup( valPairs : RDD[((Double, (Int, String)), Long)],
                        colIndexList : List[Int]){
  val n = colIndexList.last+1
  private var reduced : RDD[((Double, (Int, String)), Long)] = _
  private var sorted : RDD[((Double, (Int, String)), Long)] = _

  /**
   *
   * @return A tupples with
   *         1. KeyValueRDD of (index, group) -> Median
   *         2. Hashmap of (index, group) -> # distinct values
   */
  def getMedianAndDistinct () = {
     reduced = valPairs.reduceByKey((a, b) => a + b)
     sorted = reduced.sortByKey()
    val countAndSumByPart = calculateDistinctAndTotalForeachPart()
    val (distinct, total) = getTotalAndDistinct(countAndSumByPart)
    val half : Long = total.get(valPairs.first()._1._2).get / 2
    val mapOfLocations = getLocationsOfRanksWithinEachPart(countAndSumByPart, List(half))
    val medians = findElementsIteratively( mapOfLocations).groupByKey().mapValues(_.head)
    (medians, distinct)
  }

  def getDistinct() = {
     reduced = valPairs.reduceByKey((a, b) => a + b)
    val countsByPartition = calculateDistinctForEachPart()
    val distinct = getTotalDistinct(countsByPartition)
    distinct
  }
  /**
   * Median and distinct step 1:
   * Gets the number of distinct values and the total values by colIndex and group on each
   * partition. Doing this in two steps should prevent large shuffles across the network.
   *
   * Returns with a triple for each partition with:
   * 1. The index of the partition
   * 2. A HashMap of (colIndex, group) -> distinct values
   * 3. A HashMap of (colIndex, group) -> sum of Values
   *    (this is used in the 'getLocationsOfRanksWithinEachPart' method to calculate the medians)
   */
  protected def calculateDistinctAndTotalForeachPart() :
  Array[ (Int, HashMap[(Int, String ), Long], HashMap[(Int, String ), Long])] = {
    sorted.mapPartitionsWithIndex(
      (index : Int, it : Iterator[((Double, (Int , String ) ), Long)]) => {
      // val count = n.toLong.toInt
      val (countArray, sumArray) = GroupedPartitionProcessingUtil.totalAndDistinctByGroup(it)
      Iterator((index, countArray, sumArray))
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
   *         1. a hashmap of (index, group) -> distinctValues
   *         2. a hashmap of (index, group) -> total values
   */
  protected def getTotalAndDistinct(
                totalForEachPart : Array[(Int, HashMap[(Int,String ), Long ],
    HashMap[(Int,String ), Long ])]) : (HashMap[(Int,String ), Long ],
    HashMap[(Int,String ), Long ])  = {
    totalForEachPart.map(
      t => (t._2, t._3)).reduce(
        (hashMap1, hashMap2) => {
          val ( count1, sum1 ) = hashMap1
          val ( count2, sum2) = hashMap2
          val sumFunction = (a: Long , b: Long ) => a + b
          val mergedCounts =  HashMapUtil.mergeMaps(count1, count2, sumFunction)
          val mergedSums = HashMapUtil.mergeMaps(sum1, sum2, sumFunction)

      (mergedCounts, mergedSums)
    } )
  }

  /**
   * Median and distinct step 3:
   * Calculates the location of the desired rank statistics within each partition.
   * @param partitionMap the result of the calculateDistinctAndTotalForeachPart method
   * @param targetRanks the rank statistics we want to calculate for each index/ group. Median should
   *                    be the size of the dataset/2
   * @return An array with a HashMap for each partition of
   *         (colIndex, group) -> List of the indices of the iterator on this partition that contain
   *         the rank statics that we are looking for.
   */
  protected def getLocationsOfRanksWithinEachPart(
                partitionMap : Array[(Int, HashMap[(Int, String), Long], HashMap[(Int, String ), Long])],
                targetRanks : List[Long]
                ) :  Array[ HashMap[(Int, String ), List[Long] ]]   = {
    //as we go through the data linearly, keep track of the number of elements we have seen for
    //each partition
    var runningTotal = HashMap[(Int, String ), Long]()
    //sort by partition number
    val sortedParts = partitionMap.sortBy(_._1).map( t => (t._2, t._3))
      sortedParts.map {

        case (( countMap, sumsMap )) =>
          var relevantIndexMap = HashMap[(Int, String), List[Long] ]()
          sumsMap.foreach{
            case (((colIndex, group), colCount )) =>
            //the running totals for this column/group. If we haven't seen it yet, 0L
            val runningTotalCol = runningTotal.getOrElse((colIndex, group), 0L)
            runningTotal = runningTotal.updated((colIndex, group), runningTotalCol + colCount )
        //if the count for this column/ and group is in the right range, add it to the map
        val ranksHere = targetRanks.filter(rank =>
          runningTotalCol <= rank && runningTotalCol + colCount >= rank )

        //add ranksHere to the relevant index map
        ranksHere.foreach(
          rank => {
            val location = rank - runningTotalCol
            val indicesForThisKey = relevantIndexMap.getOrElse((colIndex, group ), List[Long]())
            relevantIndexMap = relevantIndexMap.updated(
              (colIndex, group),  location :: indicesForThisKey)
        })
      }//end sums map for each
      relevantIndexMap
    }
  }

  /**
   * Median and distinct step 4:
   * Given the location within each partition of the desired rank static, goes through the sorted
   * data an actually gets that information
   * @param locations the results of the getLocationsOfRanksWithinEachPart method
   * @return
   */
  protected def findElementsIteratively(  locations :  Array[ HashMap[(Int, String ), List[Long] ]]) = {
    sorted.mapPartitionsWithIndex((index, iter) => {
      val targetsInThisPart = locations(index)
      val len = targetsInThisPart.keys.size
        if(len >0 ) {
        val newIt = GroupedPartitionProcessingUtil.getValuesFromRanks(iter, targetsInThisPart)
        newIt
        }
      else Iterator.empty
    } )
  }

  /**
   * Calculates the number of distinct values per (index,keyPair) on each partition).
   * The per partition counts are important for the distinct median finding
   */
  protected def calculateDistinctForEachPart() :
  Array[HashMap[(Int,String ), Long ]] = {
    val zero = new HashMap[(Int, String), Long]()

    reduced.mapPartitionsWithIndex((index : Int,
                                   it : Iterator[((Double, (Int, String)), Long)]) => {
      val hashMap : HashMap[(Int, String ), Long ] = it.aggregate(zero)(
        (map : HashMap[(Int, String), Long ], v : ((Double, (Int, String)), Long) ) => {
          val ((value, (colIndex, group)) , count) = v
          val prevCount = map.getOrElse( (colIndex, group), 0L )
          //don't add the count, just as one, because we are looking for the distinct values
           map.updated((colIndex, group), prevCount + 1  )
        },
        (a , b ) => {
          val mergeFunction = (a : Long, b : Long  ) => a + b
         val h : HashMap[(Int, String ), Long ] =  HashMapUtil.mergeMaps[(Int, String ), Long](a, b , mergeFunction)
          h
        })
      Iterator(hashMap)
    }).collect()
  }

  protected def getTotalDistinct(totalForEachPart : Array[HashMap[(Int,String ), Long ]]
                        )  = {
    totalForEachPart.reduce((hashMap1, hashMap2) => HashMapUtil.mergeMaps(hashMap1,
      hashMap2, (a: Long , b: Long ) => a + b))
  }


}

/**
 * Serializable methods performed on iterators in the mapPartitions steps
 * in the above clasee
 */
object GroupedPartitionProcessingUtil extends Serializable{
  /**
   * Calculates counts an sums per group and partition
   */
  def totalAndDistinctByGroup(it : Iterator[((Double, (Int, String)), Long)] )
  :  ( HashMap[(Int, String), Long],  HashMap[(Int, String), Long]) = {

    val keyPair : ( HashMap[(Int, String), Long],  HashMap[(Int, String), Long]) =
      it.aggregate(( new HashMap[(Int, String), Long](), new HashMap[(Int, String), Long]()))(
      (acc , v : ((Double ,(Int, String )), Long)) => {
        val (countMap, sumMap ) = acc
        val ((_, (colIndex, group )) , count) = v
        val prevCount = countMap.getOrElse( (colIndex, group), 0L )
        val prevSum = sumMap.getOrElse((colIndex, group), 0L)
        //don't add the count, just as one, because we are looking for the distinct values
        val nextCounts = countMap.updated((colIndex, group), prevCount + 1  )
        val nextSums = sumMap.updated((colIndex, group), prevSum + count)
        ( nextCounts, nextSums) } ,
      (a , b ) => {
        val (aCounts, aSums) = a
        val (bCounts, bSums) = b

        val nextCounts =  HashMapUtil.mergeMaps[(Int, String), Long](aCounts, bCounts, (a :Long , b : Long  ) => a+b)
        val nextSums = HashMapUtil.mergeMaps[(Int, String), Long](aSums, bSums , (a :Long  , b : Long  ) => a+b)
        (nextCounts, nextSums)
      })
    keyPair
  }
  //return iterator of (colIndex,group)-> value, then make it into a map.
  /**
   * @param it iterator from the map partitions step in the find elements iteratively step
   * @param targetsInThisPart the results of the 'getLocationsOfRanksWithinEachPart' method
   * @return The nth value (or values, if the target ranks list is longer than one)
   *         for each column, if that nth value is in this partition.
   */
  def getValuesFromRanks(it : Iterator[((Double, (Int, String)), Long)], targetsInThisPart :HashMap[(Int, String), List[Long]] ) = {
    val keysInThisPart = targetsInThisPart.keySet
    var runningTotals = HashMap[(Int, String), Long]()

    val newIt: ArrayBuffer[((Int, String), Double)] = new scala.collection.mutable.ArrayBuffer()
    it.foreach { case (((value, key), count)) =>
      if (keysInThisPart.contains(key)) {
        val total = runningTotals.getOrElse(key, 0L)
        val ranksPresent = targetsInThisPart(key).filter(v => (v <= count + total) && (v > total))
        ranksPresent.foreach(r => {
          newIt += ((key, value))
        })
        runningTotals = runningTotals.updated(key, total + count)
      }
    }
    newIt.toIterator
  }
}
