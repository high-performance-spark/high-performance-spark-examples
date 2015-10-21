/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.functions._

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

  def happyPandas(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("place"),
      (pandaInfo("happyPandas") / pandaInfo("totalPandas")).as("percentHappy"))
  }

  def minHappyPandas(pandaInfo: DataFrame, num: Int): DataFrame = {
    pandaInfo.filter(pandaInfo("happyPandas") >= num)
  }

  //tag::maxPandaSizePerZip[]
  def maxPandaSizePerZip(pandas: DataFrame): DataFrame = {
    pandas.groupBy(pandas("zip")).max("pandasize")
  }
  //end::maxPandaSizePerZip[]

  //tag::minMaxPandasSizePerZip[]
  def minMaxPandaSizePerZip(pandas: DataFrame): DataFrame = {
    // List of strings
    pandas.groupBy(pandas("zip")).agg(("min", "pandasize"), ("max", "pandasize"))
    // Map of column to aggregate
    pandas.groupBy(pandas("zip")).agg(Map("pandasize" -> "min",
      "pandasize" -> "max"))
    // expression literals
  }
  //end::minMaxPandasSizePerZip[]

  //tag::complexAggPerZip[]
  def complexAggPerZip(pandas: DataFrame): DataFrame = {
    // Compute the min and mean
    pandas.groupBy(pandas("zip")).agg(min(pandas("pandasize")), mean(pandas("pandasize")))
  }
  //end::complexAggPerZip[]
}
