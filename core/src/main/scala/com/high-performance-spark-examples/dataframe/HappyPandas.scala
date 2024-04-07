/**
 * Happy Panda Example for DataFrames. This computes the % of happy pandas and
 * is a very contrived example (sorry!).
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.Encoders
//end::legacySparkHiveImports[]

object HappyPandas {

  /**
   * Creates a SparkSession with Hive enabled
   */
  def sparkSession(): SparkSession = {
    //tag::createSparkSession[]
    val session = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    // Import the implicits, unlike in core Spark the implicits are defined
    // on the context.
    import session.implicits._
    //end::createSparkSession[]
    session
  }

  val session = sparkSession()
  import session.implicits._

  /**
   * Creates SQLContext with an existing SparkContext.
   */
  def sqlContext(sc: SparkContext): SQLContext = {
    //tag::createSQLContext[]
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    // Import the implicits, unlike in core Spark the implicits are defined
    // on the context.
    import sqlContext.implicits._
    //end::createSQLContext[]
    sqlContext
  }

  /**
   * Creates HiveContext Spark with an existing SparkContext using hive.
   */
  def hiveContext(sc: SparkContext): SQLContext = {
    //tag::createHiveContext[]
    val hiveContext = SparkSession.builder.enableHiveSupport().getOrCreate().sqlContext
    // Import the implicits, unlike in core Spark the implicits are defined
    // on the context.
    import hiveContext.implicits._
    //end::createHiveContext[]
    hiveContext
  }

  /**
   * Illustrate loading some JSON data.
   */
  def loadDataSimple(sc: SparkContext, session: SparkSession, path: String):
      DataFrame = {
    //tag::loadPandaJSONSimple[]
    val df1 = session.read.json(path)
    //end::loadPandaJSONSimple[]
    //tag::loadPandaJSONComplex[]
    val df2 = session.read.format("json")
      .option("samplingRatio", "1.0").load(path)
    //end::loadPandaJSONComplex[]
    val jsonRDD = sc.textFile(path)
    //tag::loadPandaJsonRDD[]
    val df3 = session.read.json(session.createDataset(jsonRDD)(Encoders.STRING))
    //end::loadPandaJSONRDD[]
    df1
  }

  def jsonLoadFromRDD(session: SparkSession, input: RDD[String]): DataFrame = {
    //tag::loadPandaJSONRDD[]
    val rdd: RDD[String] = input.filter(_.contains("panda"))
    val df = session.read.json(session.createDataset(rdd)(Encoders.STRING))
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
  case class PandaInfo(
    place: String,
    pandaType: String,
    happyPandas: Integer,
    totalPandas: Integer)

  /**
    * Gets the percentage of happy pandas per place.
    *
    * @param pandaInfo the input DataFrame
    * @return Returns DataFrame of (place, percentage of happy pandas)
    */
  def happyPandasPercentage(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(
      $"place",
      ($"happyPandas" / $"totalPandas").as("percentHappy")
    )
  }

  //tag::encodePandaType[]
  /**
    * Encodes pandaType to Integer values instead of String values.
    *
    * @param pandaInfo the input DataFrame
    * @return Returns a DataFrame of pandaId and integer value for pandaType.
    */
  def encodePandaType(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select($"id",
      (when($"pt" === "giant", 0).
      when($"pt" === "red", 1).
      otherwise(2)).as("encodedType")
    )
  }
  //end::encodePandaType[]

  /**
    * Gets places with happy pandas more than minHappinessBound.
    */
  def minHappyPandas(pandaInfo: DataFrame, minHappyPandas: Int): DataFrame = {
    pandaInfo.filter($"happyPandas" >= minHappyPandas)
  }

  /**
   * Extra the panda info from panda places and compute the squisheness of the panda
   */
  def squishPandaFromPace(pandaPlace: DataFrame): DataFrame = {
    //tag::selectExplode[]
    val pandaInfo = pandaPlace.explode(pandaPlace("pandas")){
      case Row(pandas: Seq[Row]) =>
        pandas.map{
          case (Row(
            id: Long,
            zip: String,
            pt: String,
            happy: Boolean,
            attrs: Seq[Double])) =>
            RawPanda(id, zip, pt, happy, attrs.toArray)
        }}
    pandaInfo.select(
      ($"attributes"(0) / $"attributes"(1))
        .as("squishyness"))
    //end::selectExplode[]
  }

  /**
    * Find pandas that are sad
    */
  def sadPandas(pandaInfo: DataFrame): DataFrame = {
    // This one is our intentional non $ example
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
      $"happy".and($"attributes"(0) > $"attributes"(1))
    )
    //end::complexFilter[]
  }

  /**
    * Gets places that contains happy pandas more than unhappy pandas.
    */
  def happyPandasPlaces(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.filter($"happyPandas" >= $"totalPandas" / 2)
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

  def describePandas(pandas: DataFrame) = {
    //tag::pandaSizeRangeVarDescribe[]
    // Compute the count, mean, stddev, min, max summary stats for all
    // of the numeric fields of the provided panda infos. non-numeric
    // fields (such as string (name) or array types) are skipped.
    val df = pandas.describe()
    // Collect the summary back locally
    println(df.collect())
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
    pandas.groupBy(pandas("zip")).agg(
      min(pandas("pandaSize")), mean(pandas("pandaSize")))
  }
  //end::complexAggPerZip[]

  def simpleSqlExample(pandas: DataFrame): DataFrame = {
    val session = pandas.sparkSession
    //tag::pandasSQLQuery[]
    pandas.registerTempTable("pandas")
    val miniPandas = session.sql("SELECT * FROM pandas WHERE pandaSize < 12")
    //end::pandasSQLQuery[]
    miniPandas
  }

  def startJDBCServer(hiveContext: SQLContext): Unit = {
    //tag::startJDBC[]
    hiveContext.setConf("hive.server2.thrift.port", "9090")
    HiveThriftServer2.startWithContext(hiveContext)
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
    // Left semi join explicit.
    // Here we're explicit about which DF which col comes from given
    // the shared name.
    df1.join(df2, df1("name") === df2("name"), "left_semi")
    //end::leftsemiJoin[]
  }


  def badComplexJoin(df1: Dataset[Pandas], df2: Dataset[Pandas]): DataFrame = {
    df1.join(df2, regexp(df1("name"), df2("name"))).alias("regexp join")
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
