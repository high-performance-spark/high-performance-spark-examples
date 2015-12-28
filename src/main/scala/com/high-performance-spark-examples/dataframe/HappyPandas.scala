/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.thriftserver._

object HappyPanda {
  // create SQLContext with an existing SparkContext
  def sqlContext(sc: SparkContext): SQLContext = {
    //tag::createSQLContext[]
    val sqlContext = new SQLContext(sc)
    // Import the implicits, unlike in core Spark the implicits are defined on the context
    import sqlContext.implicits._
    //end::createSQLContext[]
    sqlContext
  }

  // create HiveContext with an existing SparkContext
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

  //  Here will be some examples on PandaInfo DataFrame

  /**
    * @param place name of place
    * @param pandaType type of pandas in this place
    * @param happyPandas number of happy pandas in this place
    * @param totalPandas total number of pandas in this place
    */
  case class PandaInfo(place: String, pandaType: String, happyPandas: Integer, totalPandas: Integer)

  /**
    * Gets the percentage of happy pandas per place.
    *
    * @param pandaInfo the input DataFrame
    * @return Returns a pair of (place, percentage of happy pandas)
    */
  def happyPandasPercentage(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("place"),
      (pandaInfo("happyPandas") / pandaInfo("totalPandas")).as("percentHappy"))
  }

  //tag::encodePandaType[]
  /**
    * Encodes pandaType to Integer values instead of string values.
    *
    * @param pandaInfo the input DataFrame
    * @return Returns a pair of (pandaId, mapped integer value for pandaType)
    */
  def encodePandaType(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("place"),
      (when(pandaInfo("pandaType") === "giant", 0).
      when(pandaInfo("pandaType") === "red", 1).
      otherwise(2)).as("encodedType")
    )
  }
  //end::encodePandaType[]

  //tag::simpleFilter[]
  /**
    * Gets places with happy pandas more than minHappinessBound.
    */
  def minHappyPandas(pandaInfo: DataFrame, minHappyPandas: Int): DataFrame = {
    pandaInfo.filter(pandaInfo("happyPandas") >= minHappyPandas)
  }
  //end::simpleFilter[]

  //tag::complexFilter[]
  /**
    * Gets places that contains happy pandas more than unhappy pandas.
    */
  def happyPandasPlaces(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.filter(pandaInfo("happyPandas") >= pandaInfo("totalPandas") / 2)
  }
  //end::complexFilter[]


  /**
    * @param name name of panda
    * @param zip zip code
    * @param pandaSize size of panda in KG
    * @param age age of panda
    */
  case class Pandas(name: String, zip: String, pandaSize: Integer, age: Integer)

  //tag::maxPandaSizePerZip[]
  def maxPandaSizePerZip(pandas: DataFrame): DataFrame = {
    pandas.groupBy(pandas("zip")).max("pandaSize")
  }
  //end::maxPandaSizePerZip[]

  //tag::minMaxPandasSizePerZip[]
  def minMaxPandaSizePerZip(pandas: DataFrame): DataFrame = {
    pandas.groupBy(pandas("zip")).agg(min("pandaSize"), max("pandaSize"))
  }
  //end::minMaxPandasSizePerZip[]

  def minPandaSizeMaxAgePerZip(pandas: DataFrame): DataFrame = {
    // this query can be written in two methods

    // 1
    pandas.groupBy(pandas("zip")).agg(("pandaSize", "min"), ("age", "max"))

    // 2
    pandas.groupBy(pandas("zip")).agg(Map("pandaSize" -> "min", "age" -> "max"))
  }

  //tag::complexAggPerZip[]
  def minMeanSizePerZip(pandas: DataFrame): DataFrame = {
    // Compute the min and mean
    pandas.groupBy(pandas("zip")).agg(min(pandas("pandaSize")), mean(pandas("pandaSize")))
  }
  //end::complexAggPerZip[]

  def simpleSqlExample(pandas: DataFrame): DataFrame = {
    val sqlCtx = pandas.sqlContext
    //tag::pandasSQLQuery[]
    pandas.registerTempTable("pandas")
    val miniPandas = sqlCtx.sql("SELECT * FROM pandas WHERE pandaSize < 12")
    //end::pandasSQLQuery[]
    miniPandas
  }

  def startJDBCServer(sqlContext: HiveContext): Unit = {
    //tag::startJDBC[]
    sqlContext.setConf("hive.server2.thrift.port", "9090")
    HiveThriftServer2.startWithContext(sqlContext)
    //end::startJDBC[]
  }

  /**
    * Orders pandas by size ascending and by age descending.
    * Pandas will be sorted by "size" first and if two pandas have the same "size"
    * will be sorted by "age".
    */
  def orderPandas(pandas: DataFrame): DataFrame = {
    //tag::simpleSort[]
    pandas.orderBy(pandas("pandaSize").asc, pandas("age").desc)
    //end::simpleSort[]
  }

  def computeRelativePandaSizes(pandas: DataFrame): DataFrame = {
    //tag::relativePandaSizesWindow[]
    val windowSpec = Window
      .orderBy(pandas("age"))
      .partitionBy(pandas("zip"))
      .rowsBetween(start = -1, end = 1) // use rangeBetween for range instead
    //end::relativePandaSizesWindow[]

    //tag::relativePandaSizesQuery[]
    val pandaRelativeSizeFunc = (pandas("pandaSize") -
      avg(pandas("pandaSize")).over(windowSpec))

    pandas.select(pandas("name"), pandas("zip"), pandas("pandaSize"), pandas("age"),
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
