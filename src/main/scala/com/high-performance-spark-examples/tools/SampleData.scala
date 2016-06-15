import scala.util.Random
import scala.reflect.{ClassTag}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Sample our production data to be able to use it for tests
 */
object SampleData {
  /**
   * Sample the input down to k % for usage in tests
   */
  def sampleInput[T](rdd: RDD[T]): RDD[T] = {
  // tag::randomSampleInput[]
    rdd.sample(withReplacement=false, fraction=0.1)
  // end::randomSampleInput[]
  }

  /**
   * Construct a stratified sample
   */
  def stratifiedSample(rdd: RDD[(String, Array[Double])]): RDD[(String, Array[Double])] = {
    // tag::stratifiedSample[]
    // 5% of the red pandas, and 50% of the giant pandas
    val stratas = Map("red" -> 0.05, "giant" -> 0.50)
    rdd.sampleByKey(withReplacement=false, fractions = stratas)
    // end::stratifiedSample[]
  }

  /**
   * Custom random sample with RNG. This is intended as an example of how to save setup overhead.
   */
  def slowSampleInput[T: ClassTag](rdd: RDD[T]): RDD[T] = {
    rdd.flatMap{x => val r = new Random()
      if (r.nextInt(10) == 0) {
        Some(x)
      } else {
        None
      }}
  }

  /**
   * Custom random sample with RNG. This is intended as an example of how to save setup overhead.
   */
  def customSampleInput[T: ClassTag](rdd: RDD[T]): RDD[T] = {
    // tag::mapPartitions[]
    rdd.mapPartitions{itr =>
      // Only create once RNG per partitions
      val r = new Random()
      itr.filter(x => r.nextInt(10) == 0)
    }
    // end::mapPartitions[]
  }

  // tag::broadcast[]
  class LazyPrng {
    @transient lazy val r = new Random()
  }
  def customSampleBroadcast[T: ClassTag](sc: SparkContext, rdd: RDD[T]): RDD[T]= {
    val bcastprng = sc.broadcast(new LazyPrng())
    rdd.filter(x => bcastprng.value.r.nextInt(10) == 0)
  }
  // end::broadcast[]
}
