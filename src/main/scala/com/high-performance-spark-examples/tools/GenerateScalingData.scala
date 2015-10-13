import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.random.RandomRDDs

object GenerateScalingData {
  // tag::MAGIC_PANDA[]
  /**
   * Generate a Goldilocks data set. We expect the key to follow an exponential
   * distribution and the data its self to be normal.
   */
  def generateGoldilocks(sc: SparkContext, size: Long): RDD[List[String]] = {
    val keyRDD = RandomRDDs.exponentialRDD(sc, mean = 1000,  size = size).map(_.toInt)
    val valuesRDD = RandomRDDs.normalVectorRDD(sc, numRows = 1, numCols = 500)
    keyRDD.zip(valuesRDD).map{case (k, v) => List(k.toString) ++ v.toArray.map(_.toString)}
  }
  // end::MAGIC_PANDA[]
}
