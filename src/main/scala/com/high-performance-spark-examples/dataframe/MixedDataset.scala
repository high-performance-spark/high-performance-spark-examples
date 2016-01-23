/**
 * A sample mixing relational & functional transformations with Datasets.
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Additional imports for using HiveContext
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.thriftserver._

class MixedDataset(sqlCtx: SQLContext) {
  import sqlCtx.implicits._

  /**
   * A sample function on a Dataset of RawPandas.
   * This is contrived, since our reduction could also be done with SQL aggregates, but
   * we can see the flexibility of being able to specify arbitrary Scala code.
   */
  def happyPandaSums(ds: Dataset[RawPanda]): Double = {
    ds.toDF().filter($"happy" === true).as[RawPanda].
      select($"attributes"(0).as[Double]).
      reduce((x, y) => x + y)
  }

  /**
   * Functional map + Dataset, sums the positive attributes for the pandas
   */
  def funMap(ds: Dataset[RawPanda]): Dataset[Double] = {
    ds.map{rp => rp.attributes.filter(_ > 0).sum}
  }

  /**
   * Illustrate how we make typed queries, using some of the float properties to produce boolean
   * values.
   */
  def typedQueryExample(ds: Dataset[RawPanda]): Dataset[Double] = {
    ds.select($"attributes"(0).as[Double])
  }

  /**
   * Illustrate converting a Dataset to an RDD
   */
  def toRDD(ds: Dataset[RawPanda]): RDD[RawPanda] = {
    ds.rdd
  }

  /**
   * Illustrate converting a Dataset to a DataFrame
   */
  def toDF(ds: Dataset[RawPanda]): DataFrame = {
    ds.toDF()
  }

  /**
   * Illustrate DataFrame to Dataset. Its important to note that if the schema does not match what
   * is expected by the Dataset this fails fast.
   */
  def fromDF(df: DataFrame): Dataset[RawPanda] = {
    df.as[RawPanda]
  }
}
