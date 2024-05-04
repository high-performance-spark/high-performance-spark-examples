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
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.types._

case class MiniPandaInfo(zip: String, size: Double)

class MixedDataset(sqlCtx: SQLContext) {
  import sqlCtx.implicits._

  /**
   * A sample function on a Dataset of RawPandas.
   *
   * This is contrived, since our reduction could also be done with SQL aggregates,
   * but we can see the flexibility of being able to specify arbitrary Scala code.
   */
  def happyPandaSums(ds: Dataset[RawPanda]): Double = {
    ds.toDF().filter($"happy" === true).as[RawPanda].
      select($"attributes"(0).as[Double]).
      reduce((x, y) => x + y)
  }

  /**
   * A sample function on a Dataset of RawPandas.
   * Use the first attribute to deterimine if a panda is squishy.
   */
  //tag::basicSelect[]
  def squishyPandas(ds: Dataset[RawPanda]): Dataset[(Long, Boolean)] = {
    ds.select($"id".as[Long], ($"attributes"(0) > 0.5).as[Boolean])
  }
  //end::basicSelect[]

  /**
   * Union happy and sad pandas
   */
  //tag::basicUnion[]
  def unionPandas(happyPandas: Dataset[RawPanda], sadPandas: Dataset[RawPanda]) = {
    happyPandas.union(sadPandas)
  }
  //end::basicUnion[]

  /**
   * Functional map + Dataset, sums the positive attributes for the pandas
   */
  //tag::functionalQuery[]
  def funMap(ds: Dataset[RawPanda]): Dataset[Double] = {
    ds.map{rp => rp.attributes.filter(_ > 0).sum}
  }
  //end::functionalQuery[]

  //tag::maxPandaSizePerZip[]
  def maxPandaSizePerZip(ds: Dataset[RawPanda]): Dataset[(String, Double)] = {
    ds.map(rp => MiniPandaInfo(rp.zip, rp.attributes(2)))
      .groupByKey(mp => mp.zip).agg(max("size").as[Double])
  }
  //end::maxPandaSizePerZip[]

  //tag::maxPandaSizePerZipScala[]
  def maxPandaSizePerZipScala(ds: Dataset[RawPanda]): Dataset[(String, Double)] = {
    def groupMapFun(g: String, iter: Iterator[RawPanda]): (String, Double)  = {
      (g, iter.map(_.attributes(2)).reduceLeft(Math.max(_, _)))
    }
    ds.groupByKey(rp => rp.zip).mapGroups(groupMapFun)
  }
  //end::maxPandaSizePerZipScala[]

  /**
   * Illustrate how we make typed queries, using some of the float properties
   * to produce boolean values.
   */
  def typedQueryExample(ds: Dataset[RawPanda]): Dataset[Double] = {
    ds.select($"attributes"(0).as[Double])
  }

  /**
   * Illustrate Dataset joins
   */
  def joinSample(pandas: Dataset[RawPanda], coffeeShops: Dataset[CoffeeShop]):
      Dataset[(RawPanda, CoffeeShop)] = {
    //tag::joinWith[]
    val result: Dataset[(RawPanda, CoffeeShop)] = pandas.joinWith(coffeeShops,
      pandas("zip") === coffeeShops("zip"))
    //end::joinWith[]
    result
  }

  /**
   * Illustrate a self join to compare pandas in the same zip code
   */
  def selfJoin(pandas: Dataset[RawPanda]):
      Dataset[(RawPanda, RawPanda)] = {
    //tag::selfJoin[]
    val result: Dataset[(RawPanda, RawPanda)] = pandas.as("l").joinWith(pandas.as("r"),
      $"l.zip" === $"r.zip")
    //end::selfJoin[]
    result
  }

  //tag::fromRDD[]
  /**
   * Illustrate converting an RDD to DS
   */
  def fromRDD(rdd: RDD[RawPanda]): Dataset[RawPanda] = {
    rdd.toDS
  }

  //end::fromRDD[]

  //tag::toRDDDF[]
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
  //end::toRDDDF[]

  /**
   * Illustrate DataFrame to Dataset. Its important to note that if the schema
   * does not match what is expected by the Dataset this fails fast.
   */
  //tag::DataFrameAsDataset[]
  def fromDF(df: DataFrame): Dataset[RawPanda] = {
    df.as[RawPanda]
  }
  //end::DataFrameAsDataset[]
}
