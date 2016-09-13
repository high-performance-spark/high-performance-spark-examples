package com.highperformancespark.examples.transformations

import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.reflect.ClassTag
import org.apache.spark.SparkContext._
/**
  * Created by rachelwarren on 9/11/16.
  */
object Joins {
  /**
    * Performs a broad cast hash join for two RDDs.
    * @param bigRDD - the first rdd, should be the larger RDD
    * @param smallRDD - the small rdd, should be small enough to fit in memory
    * @tparam K - The type of the key
    * @tparam V1 - The type of the values for the large array
    * @tparam V2 - The type of the values for the second array
    * @return
    */
  //tag::coreBroadCast[]
  def manualBroadCastHashJoin[K : Ordering : ClassTag, V1 : ClassTag,
  V2 : ClassTag](bigRDD : RDD[(K, V1)],
    smallRDD : RDD[(K, V2)])= {
    val smallRDDLocal: Map[K, V2] = smallRDD.collectAsMap()
    bigRDD.sparkContext.broadcast(smallRDDLocal)
    bigRDD.mapPartitions(iter => {
      iter.flatMap{
        case (k,v1 ) =>
          smallRDDLocal.get(k) match {
            case None => Seq.empty[(K, (V1, V2))]
            case Some(v2) => Seq((k, (v1, v2)))
          }
      }
    }, preservesPartitioning = true)
  }
  //end:coreBroadCast[]

}
