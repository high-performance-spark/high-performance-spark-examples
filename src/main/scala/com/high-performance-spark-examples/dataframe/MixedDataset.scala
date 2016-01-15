/**
 * A sample mixing relational & functional transformations with Datasets.
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark._
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Additional imports for using HiveContext
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.thriftserver._

object MixedDataset {
  /**
   * A sample function on a Dataset of RawPandas.
   * This is contrived, since our reduction could also be done with SQL aggregates, but
   * we can see the flexibility of being able to specify arbitrary Scala code.
   */
  def happyPandaSums() = {
  }

  /**
   * A sample function on a Dataset of RawPandas that round trips through a DataFrame for sorting.
   * In the future sorting may be available directly on Datasets but this example illustrates how to
   * convert from
   */
  def topHappyPandasSums() = {
  }

  /**
   * Illustrate how we make typed queries, using some of the float properties to produce boolean
   * values.
   */
  def typedQueryExample() = {
  }

  /**
   * Illustrate converting a Dataset to an RDD
   */
  def toRDD() = {
  }

  /**
   * Illustrate converting a Dataset to a DataFrame
   */
  def toDF() = {
  }

  /**
   * Illustrate DataFrame to Dataset. Its important to note that if the schema does not match what
   * is expected by the Dataset this fails fast.
   */
  def fromDF() = {
  }
}
