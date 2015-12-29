/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark._
//tag::sparkSQLImports[]
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//end::sparkSQLImports[]
//tag::sparkHiveImports[]
// Additional imports for using HiveContext
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.thriftserver._
//end::sparkHiveImports[]

object HappyPanda {
  // How to create a HiveContext or SQLContext with an existing SparkContext
  def sqlContext(sc: SparkContext): SQLContext = {
    //tag::createSQLContext[]
    val sqlContext = new SQLContext(sc)
    // Import the implicits, unlike in core Spark the implicits are defined on the context
    import sqlContext.implicits._
    //end::createSQLContext[]
    sqlContext
  }

  def hiveContext(sc: SparkContext): HiveContext = {
    //tag::createHiveContext[]
    val hiveContext = new HiveContext(sc)
    // Import the implicits, unlike in core Spark the implicits are defined on the context
    import hiveContext.implicits._
    //end::createHiveContext[]
    hiveContext
  }

  // Illustrate loading some JSON data
  def loadDataSimple(sc: SparkContext, sqlCtx: SQLContext, path: String): DataFrame = {
    //tag::loadPandaJSONSimple[]
    val df = sqlCtx.read.json(path)
    //end::loadPandaJSONSimple[]
    //tag::loadPandaJSONComplex[]
    val df2 = sqlCtx.read.format("json").option("samplingRatio", "1.0").load(path)
    //end::loadPandaJSONComplex[]
    val rdd = sc.textFile(path)
    //tag::loadPandaJsonRDD[]
    val df3 = sqlCtx.read.json(rdd)
    //end::loadPandaJSONRDD[]
    df
  }

  def happyPandas(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("place"),
      (pandaInfo("happyPandas") / pandaInfo("totalPandas")).as("percentHappy"))
  }

  //tag::encodePandaType[]
  def encodePandaType(pandaInfo: DataFrame, num: Int): DataFrame = {
    pandaInfo.select(pandaInfo("pandaId"),
      when(pandaInfo("pandaType") === "giant", 0).
      when(pandaInfo("pandaType") === "red", 1).
      otherwise(2)
    )
  }
  //end::encodePandaType[]

  //tag::simpleFilter[]
  def minHappyPandas(pandaInfo: DataFrame, num: Int): DataFrame = {
    pandaInfo.filter(pandaInfo("happyPandas") >= num)
  }
  //end::simpleFilter[]

  //tag::complexFilter[]
  def minHappyPandasComplex(pandaInfo: DataFrame, num: Int): DataFrame = {
    pandaInfo.filter(pandaInfo("happyPandas") >= pandaInfo("totalPandas") / 2)
  }
  //end::complexFilter[]

  //tag::maxPandaSizePerZip[]
  def maxPandaSizePerZip(pandas: DataFrame): DataFrame = {
    pandas.groupBy(pandas("zip")).max("pandaSize")
  }
  //end::maxPandaSizePerZip[]

  //tag::minMaxPandasSizePerZip[]
  def minMaxPandaSizePerZip(pandas: DataFrame): DataFrame = {
    // List of strings
    pandas.groupBy(pandas("zip")).agg(("min", "pandaSize"), ("max", "pandaSize"))
    // Map of column to aggregate
    pandas.groupBy(pandas("zip")).agg(Map("pandaSize" -> "min",
      "pandaSize" -> "max"))
    // expression literals
  }
  //end::minMaxPandasSizePerZip[]

  //tag::complexAggPerZip[]
  def complexAggPerZip(pandas: DataFrame): DataFrame = {
    // Compute the min and mean
    pandas.groupBy(pandas("zip")).agg(min(pandas("pandaSize")), mean(pandas("pandaSize")))
  }
  //end::complexAggPerZip[]

  def simpleSqlExample(pandas: DataFrame): DataFrame = {
    val sqlCtx = pandas.sqlContext
    //tag::pandasSQLQuery[]
    pandas.registerTempTable("pandas")
    val miniPandas = sqlCtx.sql("SELECT * FROM pandas WHERE pandaSize < 100")
    //end::pandasSQLQuery[]
    miniPandas
  }

  def startJDBCServer(sqlContext: HiveContext): Unit = {
    //tag::startJDBC[]
    sqlContext.setConf("hive.server2.thrift.port", "9090")
    HiveThriftServer2.startWithContext(sqlContext)
    //end::startJDBC[]
  }

  def orderPandas(pandas: DataFrame): DataFrame = {
    //tag::simpleSort[]
    pandas.orderBy(pandas("size").asc, pandas("age").desc)
    //end::simpleSort[]
  }

  def computeRelativePandaSizes(pandas: DataFrame): DataFrame = {
    //tag::relativePandaSizesWindow[]
    val windowSpec = Window
      .orderBy(pandas("age"))
      .partitionBy(pandas("zip"))
      .rowsBetween(start = 10, end = 10) // use rangeBetween for range instead
    //end::relativePandaSizesWindow[]
    //tag::relativePandaSizesQuery[]
    val pandaRelativeSizeFunc = (pandas("pandaSize") -
      avg(pandas("pandaSize")).over(windowSpec))
    pandas.select(pandas("name"), pandas("zip"), pandas("pandaSize"),
      pandaRelativeSizeFunc.as("panda_relative_size"))
    //end::relativePandaSizesQuery[]
  }

  // Join DataFrames of Pandas and Sizes with
  def joins(df1: DataFrame, df2: DataFrame): Unit = {
    // Inner join implicit
    //tag::innerJoin[]
    df1.join(df2, df1("name") === df2("name"))
    //end::innerJoin[]
    //tag::joins[]
    // Inner join explicit
    df1.join(df2, df1("name") === df2("name"), "inner")
    // Left outer join explicit
    df1.join(df2, df1("name") === df2("name"), "left_outer")
    // Right outer join explicit
    df1.join(df2, df1("name") === df2("name"), "right_outer")
    // Left semi join explicit
    df1.join(df2, df1("name") === df2("name"), "leftsemi")
    //end::joins[]
  }
}
