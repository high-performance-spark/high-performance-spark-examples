/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark._
import org.apache.spark.rdd.RDD
//tag::sparkSQLImports[]
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
//end::sparkSQLImports[]

//tag::sparkHiveImports[]
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver._
//end::sparkHiveImports[]

object HappyPandas {
  /**
   * Creates SQLContext with an existing SparkContext.
   */
  def sqlContext(sc: SparkContext): SQLContext = {
    //tag::createSQLContext[]
    val sqlContext = new SQLContext(sc)
    // Import the implicits, unlike in core Spark the implicits are defined on the context
    import sqlContext.implicits._
    //end::createSQLContext[]
    sqlContext
  }

  /**
   * Creates Spark Session with an existing SparkContext using hive.
   */
  def sparkSession(sc: SparkContext): SparkSession = {
    //tag::createSession[]
    val session = SparkSession.builder().enableHiveSupport().getOrCreate()
    // Import the implicits, unlike in core Spark the implicits are defined on the context
    import session.implicits._
    //end::createSession[]
    session
  }

  /**
   * Creates HiveContext Spark with an existing SparkContext using hive.
   */
  def hiveContext(sc: SparkContext): HiveContext = {
    //tag::createHiveContext[]
    val hiveContext = new HiveContext(sc)
    // Import the implicits, unlike in core Spark the implicits are defined on the context
    import hiveContext.implicits._
    //end::createHiveContext[]
    hiveContext
  }

  /**
   * Illustrate loading some JSON data.
   */
  def loadDataSimple(sc: SparkContext, sqlCtx: SQLContext, path: String): DataFrame = {
    //tag::loadPandaJSONSimple[]
    val df1 = sqlCtx.read.json(path)
    //end::loadPandaJSONSimple[]
    //tag::loadPandaJSONComplex[]
    val df2 = sqlCtx.read.format("json").option("samplingRatio", "1.0").load(path)
    //end::loadPandaJSONComplex[]
    val jsonRDD = sc.textFile(path)
    //tag::loadPandaJsonRDD[]
    val df3 = sqlCtx.read.json(jsonRDD)
    //end::loadPandaJSONRDD[]
    df1
  }

  def jsonLoadFromRDD(sqlCtx: SQLContext, input: RDD[String]): DataFrame = {
    //tag::loadPandaJSONRDD[]
    val rdd: RDD[String] = input.filter(_.contains("panda"))
    val df = sqlCtx.read.json(rdd)
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
    * @return Returns DataFrame of (place, percentage of happy pandas)
    */
  def happyPandasPercentage(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("place"), (pandaInfo("happyPandas") / pandaInfo("totalPandas")).as("percentHappy"))
  }

  //tag::encodePandaType[]
  /**
    * Encodes pandaType to Integer values instead of String values.
    *
    * @param pandaInfo the input DataFrame
    * @return Returns a DataFrame of pandaId and integer value for pandaType.
    */
  def encodePandaType(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("id"),
      (when(pandaInfo("pt") === "giant", 0).
      when(pandaInfo("pt") === "red", 1).
      otherwise(2)).as("encodedType")
    )
  }
  //end::encodePandaType[]

  /**
    * Gets places with happy pandas more than minHappinessBound.
    */
  def minHappyPandas(pandaInfo: DataFrame, minHappyPandas: Int): DataFrame = {
    pandaInfo.filter(pandaInfo("happyPandas") >= minHappyPandas)
  }

  /**
   * Extra the panda info from panda places and compute the squisheness of the panda
   */
  def squishPandaFromPace(pandaPlace: DataFrame): DataFrame = {
    //tag::selectExplode[]
    val pandaInfo = pandaPlace.explode(pandaPlace("pandas")){
      case Row(pandas: Seq[Row]) =>
        pandas.map{
          case Row(id: Long, zip: String, pt: String, happy: Boolean, attrs: Seq[Double]) =>
            RawPanda(id, zip, pt, happy, attrs.toArray)
        }}
    pandaInfo.select(
      (pandaInfo("attributes")(0) / pandaInfo("attributes")(1))
        .as("squishyness"))
    //end::selectExplode[]
  }

  /**
    * Find pandas that are sad
    */
  def sadPandas(pandaInfo: DataFrame): DataFrame = {
    //tag::simpleFilter[]
    pandaInfo.filter(pandaInfo("happy") !== true)
    //end::simpleFilter[]
  }

  /**
   * Find pandas that are happy and fuzzier than squishy.
   */
  def happyFuzzyPandas(pandaInfo: DataFrame): DataFrame = {
    //tag::complexFilter[]
    pandaInfo.filter(
      pandaInfo("happy").and(pandaInfo("attributes")(0) > pandaInfo("attributes")(1))
    )
    //end::complexFilter[]
  }

  /**
    * Gets places that contains happy pandas more than unhappy pandas.
    */
  def happyPandasPlaces(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.filter(pandaInfo("happyPandas") >= pandaInfo("totalPandas") / 2)
  }


  /**
   * Remove duplicate pandas by id.
   */
  def removeDuplicates(pandas: DataFrame): DataFrame = {
    //tag::dropDuplicatePandaIds[]
    pandas.dropDuplicates(List("id"))
    //end::dropDuplicatePandaIds[]
  }

  /**
    * @param name name of panda
    * @param zip zip code
    * @param pandaSize size of panda in KG
    * @param age age of panda
    */
  case class Pandas(name: String, zip: String, pandaSize: Integer, age: Integer)

  def describePandas(pandas: DataFrame): DataFrame = {
    //tag::pandaSizeRangeVarDescribe[]
    pandas.describe()
    //end::pandaSizeRangeVarDescribe[]
  }

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
      .rowsBetween(start = -10, end = 10) // can use rangeBetween for range instead
    //end::relativePandaSizesWindow[]

    //tag::relativePandaSizesQuery[]
    val pandaRelativeSizeCol = pandas("pandaSize") -
      avg(pandas("pandaSize")).over(windowSpec)

    pandas.select(pandas("name"), pandas("zip"), pandas("pandaSize"), pandas("age"),
      pandaRelativeSizeCol.as("panda_relative_size"))
    //end::relativePandaSizesQuery[]
  }

  // Join DataFrames of Pandas and Sizes with
  def joins(df1: DataFrame, df2: DataFrame): Unit = {

    //tag::innerJoin[]
    // Inner join implicit
    df1.join(df2, df1("name") === df2("name"))
    // Inner join explicit
    df1.join(df2, df1("name") === df2("name"), "inner")
    //end::innerJoin[]

    //tag::leftouterJoin[]
    // Left outer join explicit
    df1.join(df2, df1("name") === df2("name"), "left_outer")
    //end::leftouterJoin[]

    //tag::rightouterJoin[]
    // Right outer join explicit
    df1.join(df2, df1("name") === df2("name"), "right_outer")
    //end::rightouterJoin[]

    //tag::leftsemiJoin[]
    // Left semi join explicit
    df1.join(df2, df1("name") === df2("name"), "left_semi")
    //end::leftsemiJoin[]
  }

  /**
   * Cut the lineage of a DataFrame which has too long a query plan.
   */
  def cutLineage(df: DataFrame): DataFrame = {
    val sqlCtx = df.sqlContext
    //tag::cutLineage[]
    val rdd = df.rdd
    rdd.cache()
    sqlCtx.createDataFrame(rdd, df.schema)
    //end::cutLineage[]
  }

  // Self join
  def selfJoin(df: DataFrame): DataFrame = {
    val sqlCtx = df.sqlContext
    import sqlCtx.implicits._
    //tag::selfJoin[]
    val joined = df.as("a").join(df.as("b")).where($"a.name" === $"b.name")
    //end::selfJoin[]
    joined
  }
}
