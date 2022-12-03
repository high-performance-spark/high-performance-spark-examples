/**
 * Illustrates how to use Spark accumulators. Note that most of these examples
 * are "dangerous" in that they may not return consistent results.
 */
package com.highperformancespark.examples.transformations

import java.{lang => jl}

import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.util.AccumulatorV2

import com.highperformancespark.examples.dataframe.RawPanda
object Accumulators {
  /**
   * Compute the total fuzzyness with an accumulator while generating
   * an id and zip pair for sorting.
   */
  //tag::sumFuzzyAcc[]
  def computeTotalFuzzyNess(sc: SparkContext, rdd: RDD[RawPanda]):
      (RDD[(String, Long)], Double) = {
    // Create an accumulator with the initial value of 0.0
    val acc = sc.doubleAccumulator
    val transformed = rdd.map{x => acc.add(x.attributes(0)); (x.zip, x.id)}
    // accumulator still has zero value
    // Note: This example is dangerous since the transformation may be
    // evaluated multiple times.
    transformed.count() // force evaluation
    (transformed, acc.value)
  }
  //end::sumFuzzyAcc[]

  /**
   * Compute the max fuzzyness with an accumulator while generating an
   * id and zip pair for sorting.
   */
  //tag::maxFuzzyAcc[]
  def computeMaxFuzzyNess(sc: SparkContext, rdd: RDD[RawPanda]):
      (RDD[(String, Long)], Double) = {
    class MaxDoubleParam extends AccumulatorV2[jl.Double, jl.Double] {
      var _value = Double.MinValue
      override def isZero(): Boolean = {
        _value == Double.MinValue
      }
      override def reset() = {
        _value = Double.MinValue
      }

      override def add(r1: jl.Double): Unit = {
        _value = Math.max(r1, _value)
      }

      def add(r1: Double): Unit = {
        _value = Math.max(r1, _value)
      }

      def copy(): MaxDoubleParam = {
        val newAcc = new MaxDoubleParam()
        newAcc._value = _value
        newAcc
      }

      override def merge(other: AccumulatorV2[jl.Double, jl.Double]): Unit = other match {
        case o: MaxDoubleParam =>
          _value = Math.max(_value, o._value)
        case _ =>
          throw new UnsupportedOperationException(
            s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
      }

      override def value: jl.Double = _value
    }
    // Create an accumulator with the initial value of Double.MinValue
    val acc = new MaxDoubleParam()
    sc.register(acc)
    val transformed = rdd.map{x => acc.add(x.attributes(0)); (x.zip, x.id)}
    // accumulator still has Double.MinValue
    // Note: This example is dangerous since the transformation may be
    // evaluated multiple times.
    transformed.count() // force evaluation
    (transformed, acc.value)
  }
  //end::maxFuzzyAcc[]

  //tag::uniquePandaAcc[]
  def uniquePandas(sc: SparkContext, rdd: RDD[RawPanda]): HashSet[Long] = {
    object UniqParam extends AccumulableParam[HashSet[Long], Long] {
      override def zero(initValue: HashSet[Long]) = initValue
      // For adding new values
      override def addAccumulator(r: HashSet[Long], t: Long): HashSet[Long] = {
        r += t
        r
      }
      // For merging accumulators
      override def addInPlace(r1: HashSet[Long], r2: HashSet[Long]):
          HashSet[Long] = {
        r1 ++ r2
      }
    }
    // Create an accumulator with the initial value of Double.MinValue
    val acc = sc.accumulable(new HashSet[Long]())(UniqParam)
    val transformed = rdd.map{x => acc += x.id; (x.zip, x.id)}
    // accumulator still has Double.MinValue
    transformed.count() // force evaluation
    acc.value
  }
  //end::uniquePandaAcc[]
}
