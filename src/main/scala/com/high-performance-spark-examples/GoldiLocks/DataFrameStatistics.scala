
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.HashMap

/**
 * Methods for computing statics on columns in a dataFrame
 */
object DataFrameStatistics {
  //toDO: Right now all the keys have to fit in memory... maybe that is a bad idea?

  def findMedianAndDistinctByKey(inputData: DataFrame, medianColumns: List[Int],
                                 distinctColumns: List[Int], groupByIndices: List[Int]) = {
    val allColumns = (medianColumns ++ distinctColumns).distinct
    val valPairs = DataFrameProcessingUtils.createHashMapBySeveralGroups(inputData,
      allColumns, groupByIndices)
    val calculator = new RankStatsByGroup(valPairs, allColumns)
    val (mapOfMedians, distinct) = calculator.getMedianAndDistinct()
    val medianCols = mapMediansToWide(mapOfMedians, medianColumns)
    //add the distinct values
    medianCols.map{ case ((group, ar )) =>
      val distinctRow = distinctColumns.map( i => distinct.getOrElse((i, group), -1L ) )
      (group , (ar, distinctRow))
    }
  }

  /**
   * Maps the result of the median to one row per group
   */
  private def mapMediansToWide(
                        medianMap: RDD[((Int, String),
                          Double )], medians: List[Int]
                        ): RDD[(String, Array[Double])] = {
    val locationMedianIndexMap = medians.zipWithIndex.toMap
    medianMap.sparkContext.broadcast(locationMedianIndexMap)
    val zero = Array.fill[Double](medians.length)(Double.NaN)
    val useGroupAsKey = medianMap.mapPartitions(
      iter => iter.map{
        case (((index, group), median)) =>
          (group, (locationMedianIndexMap.get(index), median ))
      }.filter(
          x => x._2._1.isDefined
        ).map{
        case ((group, (Some(i), v ))) => (group, (i, v))
      }
    )
    useGroupAsKey.aggregateByKey(zero)(
      seqOp =
        (row, x) => {
        val (index, value) = x
        row.updated(index, value)
      },
      combOp =
        (a, b) => a.zip(b).map { case ((x, y)) =>
        val (xNan, yNan) = (x.isNaN, y.isNaN)
        (xNan, yNan) match {
          case (true, true) => Double.NaN
          case (false, true) => x
          case (true, false) => y
          //this is a non exhaustive match, if both x and y have values we should  throw exception }
        }
      })
  }
}

/**
 * Methods to process data frames to key/value pairs.
 */
object DataFrameProcessingUtils{
  //this delim is used to seperate the keys, its ugly but it works
  val keyDelim : String  = "_alpine><Delim_"
  def createHashMap(inputData : DataFrame, activeCols : List[Int]) : RDD[((Double, Int), Long)] = {
    val map =  inputData.rdd.mapPartitions(it => {
      val hashMap = new collection.mutable.HashMap[(Double, Int), Long]()
      it.foreach( row => {
        activeCols.foreach( i => {
          val v = row.get(i).toString.toDouble
          val key = (v, i)
          val count = hashMap.getOrElseUpdate(key, 0)
          hashMap.update(key, count + 1 )
        })
      })
      val newM = hashMap.toArray
      newM.toIterator
    })
    map
  }

  /**
   * @param groupByIndex the index of the groupBy Column
   * @return rdd with ((cellValue, (cellIndex, groupByValue)), count)
   */
  def createHashMapByGroup(inputData : DataFrame,
                           activeCols : Seq[Int], groupByIndex : Int
                            ) : RDD[((Double, (Int, String)), Long)] = {
    val map =  inputData.rdd.mapPartitions(it => {
      val hashMap = new collection.mutable.HashMap[(Double, (Int, String)), Long]()
      it.foreach( row => {
        val groupKey = row.get(groupByIndex).toString
        activeCols.foreach( i => {
          val v = row.get(i).toString.toDouble
          val key = (v, (i, groupKey))
          val count = hashMap.getOrElseUpdate(key, 0)
          hashMap.update(key, count + 1 )
        })
      })
      val newM = hashMap.toArray
      newM.toIterator
    })
    map
  }

  /**
   * Creates an
   * @param groupByIndices the index of the groupBy Column
   * @return rdd with ((cellValue, (cellIndex, groupByValue)), count)
   */
  def createHashMapBySeveralGroups(inputData : DataFrame,
                           activeCols : Seq[Int], groupByIndices : List[Int]
                            ) : RDD[((Double, (Int, String)), Long)] = {
    val map =  inputData.rdd.mapPartitions(it => {
      val hashMap = new collection.mutable.HashMap[(Double, (Int, String)), Long]()
      it.foreach( row => {
        val groupKey = buildKey(row, groupByIndices)
        activeCols.foreach( i => {
          val v = row.get(i).toString.toDouble
          val key = (v, (i, groupKey))
          val count = hashMap.getOrElseUpdate(key, 0)
          hashMap.update(key, count + 1 )
        })
      })
      val newM = hashMap.toArray
      newM.toIterator
    })
    map
  }

  /**
   * * Used in for the MultivariateStatisticalSumerizer  methods.
   * Maps a data frame to key value pair of (group, vector)

   * @param inputData the data frame
   * @param activeCols the columns that will be used in the operation
   * @param groupByIndices the indices of the group by columns from which we will build the key
   * @return
   */
  def mapToKeyedVectors(inputData : DataFrame, activeCols : Array[Int], groupByIndices : List[Int ]) = {
     inputData.map( row => {
      val key = buildKey(row, groupByIndices)
      val vector =  org.apache.spark.mllib.linalg.Vectors.dense(activeCols.map(
        i => row.get(i).toString.toDouble)
      )
      (key, vector)
    })
  }

  /**
   * Build the key for the vector or HashMap by combining  the values of the group by columns
   * at each row.
   * Uses the special delimiter field of this class.
   */
  protected def buildKey(row  : Row , groupByKeys : List[Int]) : String = {
    val key : StringBuilder = new StringBuilder
    key.append(row.get(groupByKeys.head).toString)
    groupByKeys.tail.foreach( index => {
      key.append(keyDelim + row.get(index).toString )
    })
    key.toString()
  }

  /**
   * Splits the concatenated key into an array
   */
  def getKey(key : String ): Array[String] ={
    val s = key.split(keyDelim)
    if(s.isEmpty) Array(key) else s
  }
}

/**
 * Useful when using HashMaps as Accumulators
 */
object HashMapUtil extends Serializable {
  def mergeMaps[A, B](
                       mapA : HashMap[A, B],
                       mapB : HashMap[A, B],
                       merge : (B,B)=> B) : HashMap[A,B] = {
    var nextMap = mapB
    //add thing to map B
    mapA.foreach{case ((key, aValue)) =>
      val bValue = mapB.get(key)
      nextMap =  bValue match {
        case (Some(v)) => nextMap.updated(key, merge(aValue, v))
        case None => nextMap.updated(key, aValue)
      }
    }
    nextMap
  }
}