/**
 * Illustrates how to use Spark accumulators with the "new" V2 APIs.
 *
 * Note that most of these examples are "dangerous" in that they may
 * not return consistent results.
 */
package com.highperformancespark.examples.transformations

import com.highperformancespark.examples.dataframe.RawPanda

import org.apache.spark._
//tag::import[]
import org.apache.spark.util.AccumulatorV2
//end::import[]
import org.apache.spark.rdd._

import scala.collection.mutable.HashSet
object NewAccumulators {
  /**
   * Compute the total fuzzyness with an accumulator while generating
   * an id and zip pair for sorting.
   */
  //tag::sumFuzzyAcc[]
  def computeTotalFuzzyNess(sc: SparkContext, rdd: RDD[RawPanda]):
      (RDD[(String, Long)], Double) = {
    // Create an named accumulator for doubles
    val acc = sc.doubleAccumulator("fuzzyNess")
    val transformed = rdd.map{x => acc.add(x.attributes(0)); (x.zip, x.id)}
    // accumulator still has zero value
    // Note: This example is dangerous since the transformation may be
    // evaluated multiple times.
    transformed.count() // force evaluation
    (transformed, acc.value)
  }
  //end::sumFuzzyAcc[]

  /**
   * Compute the max fuzzyness with an accumulator while generating
   * an id and zip pair for sorting.
   */
  //tag::maxFuzzyAcc[]
  def computeMaxFuzzyNess(sc: SparkContext, rdd: RDD[RawPanda]):
      (RDD[(String, Long)], Option[Double]) = {
    class MaxDoubleAccumulator extends AccumulatorV2[Double, Option[Double]] {
      // Here is the var we will accumulate our value in to.
      var currentVal: Option[Double] = None
      override def isZero = currentVal.isEmpty

      // Reset the current accumulator to zero - used when sending over the wire
      // to the workers.
      override def reset() = {
        currentVal = None
      }

      // Copy the current accumulator - this is only realy used in context of
      // copy and reset - but since its part of the public API lets be safe.
      def copy() = {
        val newCopy = new MaxDoubleAccumulator()
        newCopy.currentVal = currentVal
        newCopy
      }

      // We override copy and reset for "speed" - no need to copy the value if
      // we care going to zero it right away. This doesn't make much difference
      // for Option[Double] but for something like Array[X] could be huge.

      override def copyAndReset() = {
        new MaxDoubleAccumulator()
      }

      // Add a new value (called on the worker side)
      override def add(value: Double) = {
        currentVal = Some(
          // If the value is present compare it to the new value - otherwise
          // just store the new value as the current max.
          currentVal.map(acc => Math.max(acc, value)).getOrElse(value))
      }

      override def merge(other: AccumulatorV2[Double, Option[Double]]) = {
        other match {
          case otherFuzzy: MaxDoubleAccumulator =>
            // If the other accumulator has the option set merge it in with
            // the standard add procedure. If the other accumulator isn't set
            // do nothing.
            otherFuzzy.currentVal.foreach(value => add(value))
          case _ =>
            // This should never happen, Spark will only call merge with
            // the correct type - but that won't stop someone else from calling
            // merge so throw an exception just in case.
            throw new Exception("Unexpected merge with unsupported type" + other)
        }
      }
      // Return the accumulated value.
      override def value = currentVal
    }
    // Create a new custom accumulator
    val acc = new MaxDoubleAccumulator()
    sc.register(acc)
    val transformed = rdd.map{x => acc.add(x.attributes(0)); (x.zip, x.id)}
    // accumulator still has None value.
    // Note: This example is dangerous since the transformation may be
    // evaluated multiple times.
    transformed.count() // force evaluation
    (transformed, acc.value)
  }
  //end::maxFuzzyAcc[]

  //tag::uniquePandaAcc[]
  def uniquePandas(sc: SparkContext, rdd: RDD[RawPanda]): HashSet[Long] = {
    class UniqParam extends AccumulatorV2[Long, HashSet[Long]] {
      var accValue: HashSet[Long] = new HashSet[Long]()

      def value = accValue

      override def copy() = {
        val newCopy = new UniqParam()
        newCopy.accValue = accValue.clone
        newCopy
      }
      override def reset() = {
        this.accValue = new HashSet[Long]()
      }
      override def isZero() = {
        accValue.isEmpty
      }

      // We override copy and reset for speed - no need to copy the value if
      // we care going to zero it right away.
      override def copyAndReset() = {
        new UniqParam()
      }
      // For adding new values
      override def add(value: Long) = {
        accValue += value
      }
      // For merging accumulators
      override def merge(other: AccumulatorV2[Long, HashSet[Long]]) = {
        other match {
          case otherUniq: UniqParam =>
            accValue = accValue ++ otherUniq.accValue
          case _ =>
            throw new Exception("only support merging with same type")
        }
      }
    }
    // Create an accumulator for keeping track of unique values
    val acc = new UniqParam()
    // Register with a name
    sc.register(acc, "Unique values")
    val transformed = rdd.map{x => acc.add(x.id); (x.zip, x.id)}
    // accumulator still has Double.MinValue
    transformed.count() // force evaluation
    acc.value
  }
  //end::uniquePandaAcc[]
}
