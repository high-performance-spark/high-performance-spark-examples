package  com.highperformancespark.examples.goldilocks

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

object PandaSecondarySort {

  //Sort first by panda Id (a tuple of four things). Then by city, zip, and name ,
  // Name, address, zip, happiness

  //We want to sort the pandas by location and then by names
   def secondarySort(rdd : RDD[(String, StreetAddress, Int, Double)]) = {
    val keyedRDD: RDD[(PandaKey, (String, StreetAddress, Int, Double))] = rdd.map {
      case (fullName, address, zip, happiness) =>
        (PandaKey(address.city, zip, address.houseNumber, fullName),
          (fullName, address, zip, happiness))
    }

     //tag::implicitOrdering[]
    implicit def orderByLocationAndName[A <: PandaKey]: Ordering[A] = {
      Ordering.by(pandaKey => (pandaKey.city, pandaKey.zip, pandaKey.name))
    }
    //end::implicitOrdering[]

    keyedRDD.sortByKey().values
  }

  def groupByCityAndSortWithinGroups(rdd : RDD[(String, StreetAddress, Int, Double)]) = {
    val keyedRDD: RDD[(PandaKey, (String, StreetAddress, Int, Double))] = rdd.map {
      case (fullName, address, zip, happiness) =>
        (PandaKey(address.city, zip, address.houseNumber, fullName),
          (fullName, address, zip, happiness))
    }

    val pandaPartitioner = new PandaKeyPartitioner(rdd.partitions.length)
    implicit def orderByLocationAndName[A <: PandaKey]: Ordering[A] = {
      Ordering.by(pandaKey => (pandaKey.city, pandaKey.zip, pandaKey.name))
    }
    keyedRDD.repartitionAndSortWithinPartitions(pandaPartitioner)
    val sortedOnPartitions: RDD[(PandaKey, (String, StreetAddress, Int, Double))] = keyedRDD.repartitionAndSortWithinPartitions(pandaPartitioner)
    sortedOnPartitions.mapPartitions(
      iter => {
      val typedIter
      = iter.map(x => (x, 1))
        SecondarySort.groupSorted(typedIter)
      })
  }
}

case class PandaKey(city : String, zip : Int, addressNumber : Long, name : String )
case class StreetAddress(city : String, streetName : String, houseNumber : Long )

class PandaKeyPartitioner(override val numPartitions: Int) extends Partitioner {
  require(numPartitions >= 0, s"Number of partitions ($numPartitions) cannot be negative.")

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[PandaKey]
     Math.abs(k.city.hashCode) % numPartitions //hashcode of city
  }
}


object SecondarySort {

  //tag::sortByTwoKeys[]
  def sortByTwoKeys[K : Ordering : ClassTag , S, V : ClassTag](
    pairRDD : RDD[((K, S), V)], partitions : Int ) = {
    val colValuePartitioner = new PrimaryKeyPartitioner[K, S](partitions)

   //tag::implicitOrdering[]
    implicit val ordering: Ordering[(K, S)] = Ordering.by(_._1)
    //end::implicitOrdering[]
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
      case Nil =>
        val ((firstKey, secondKey), value) = next
        List((firstKey, ArrayBuffer((secondKey, value))))

      case head :: rest =>
        val (curKey, valueBuf) = head
        val ((firstKey, secondKey), value) = next
        if (!firstKey.equals(curKey) ) {
          (firstKey, ArrayBuffer((secondKey, value))) :: list
        } else {
          valueBuf.append((secondKey, value))
          list
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

  def coLocated(a : RDD[(Int, String)], b : RDD[(Int, String)],
    partitionerX : Partitioner, partitionerY :Partitioner): Unit = {

    //tag::coLocated[]
    val rddA = a.partitionBy(partitionerX)
    rddA.cache()
    val rddB = b.partitionBy(partitionerY)
    rddB.cache()
    val rddC = a.cogroup(b)
    rddC.count()
    //end::coLocated[]
    }

  def notCoLocated(a : RDD[(Int, String)], b : RDD[(Int, String )],
    partitionerX : Partitioner, partitionerY :Partitioner): Unit = {

    //tag::notCoLocated[]
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
