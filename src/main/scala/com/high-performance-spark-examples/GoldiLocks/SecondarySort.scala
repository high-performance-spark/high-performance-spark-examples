package  com.highperformancespark.examples.goldilocks

import org.apache.spark.{Partitioner, HashPartitioner}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SecondarySort {

  //tag::sortByTwoKeys[]
  def sortByTwoKeys[K : Ordering : ClassTag , S, V : ClassTag](pairRDD : RDD[((K, S), V)], partitions : Int ) = {
    val colValuePartitioner = new PrimaryKeyPartitioner[K, S](partitions)
    implicit val ordering: Ordering[(K, S)] = Ordering.by(_._1)
    val sortedWithinParts = pairRDD.repartitionAndSortWithinPartitions(
      colValuePartitioner)
    sortedWithinParts
  }
  //end::sortByTwoKeys[]

  //tag::sortAndGroup[]
  def groupByKeyAndSortBySecondaryKey[K : Ordering : ClassTag, S, V : ClassTag](pairRDD : RDD[((K, S), V)], partitions : Int ) = {
    val colValuePartitioner = new PrimaryKeyPartitioner[Double, Int](partitions)
    implicit val ordering: Ordering[(K, S)] = Ordering.by(_._1)
    val sortedWithinParts = pairRDD.repartitionAndSortWithinPartitions(
      colValuePartitioner)
    sortedWithinParts.mapPartitions( iter => groupSorted[K, S, V](iter) )
  }

  def groupSorted[K,S,V](
    it: Iterator[((K, S), V)]): Iterator[(K, List[(S, V)])] = {
    val res = List[(K, ArrayBuffer[(S, V)])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil => {
        val ((firstKey, secondKey), value) = next
        List((firstKey, ArrayBuffer((secondKey, value))))
      }
      case head :: rest => {
        val (curKey, valueBuf) = head
        val ((firstKey, secondKey), value) = next
        if (!firstKey.equals(curKey) ) {
          (firstKey, ArrayBuffer((secondKey, value))) :: list
        } else {
          valueBuf.append((secondKey, value))
          list
        }
      }
    }).map { case (key, buf) => (key, buf.toList) }.iterator
  }
  //end::sortAndGroup[]

}

//tag::primaryKeyPartitioner[]
class PrimaryKeyPartitioner[K, S](partitions: Int) extends Partitioner {
  /**
   * We create a hash partitioner and use it with the first set of keys.
   */
  val delegatePartitioner = new HashPartitioner(partitions)

  override def numPartitions = delegatePartitioner.numPartitions

  /**
   * Partition according to the hash value of the first key
   */
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(K, S)]
    delegatePartitioner.getPartition(k._1)
  }
}
//end::primaryKeyPartitioner[]

object CoPartitioningLessons {

  def coLocated[K,V](a : RDD[(K, V)], b : RDD[(K, V)],
    partitionerX : Partitioner, partitionerY :Partitioner): Unit = {

    //tag::coLocated
    val rddA = a.partitionBy(partitionerX)
    rddA.cache()
    val rddB = b.partitionBy(partitionerY)
    rddB.cache()
    val rddC = a.cogroup(b)
    rddC.count()
    //end::coLocated[]
    }

  def notCoLocated[K,V](a : RDD[(K, V)], b : RDD[(K, V)],
    partitionerX : Partitioner, partitionerY :Partitioner): Unit = {

    //tag::notCoLocated
    val rddA = a.partitionBy(partitionerX)
    rddA.cache()
    val rddB = b.partitionBy(partitionerY)
    rddB.cache()
    val rddC = a.cogroup(b)
    rddA.count()
    rddB.count()
    rddC.count()
    //end::notCoLocated[]
    }
}
