/**
 * Happy Panda Example for DataFrames.
 * Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{lower, rand}
import org.apache.spark.sql.types._

import com.highperformancespark.examples.dataframe.HappyPandas.PandaInfo
import com.highperformancespark.examples.dataframe.HappyPandas.Pandas
import com.holdenkarau.spark.testing._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class SQLExtensionTest extends AnyFunSuite with ScalaDataFrameSuiteBase {

  val rawPandaList = List(
    RawPanda(10L, "94110", "giant", true, Array(1.0, 0.9)),
    RawPanda(11L, "94110", "red", true, Array(1.0, 0.9)))

  override def conf: SparkConf = {
    val initialConf = super.conf
    initialConf.set(
      "spark.sql.extensions",
      "com.highperformancespark.examples.dataframe.SQLExtension")
  }

  def explainToString(df: DataFrame): String = {
    df.queryExecution.explainString(ExplainMode.fromString("extended"))
  }

  test("Magic") {
    import spark.implicits._
    val inputDF = spark.createDataFrame(rawPandaList)
    spark.sql("DROP TABLE IF EXISTS farts")
    inputDF.write.saveAsTable("farts")
    val testDF = spark.read.table("farts")
    val explained: String = explainToString(testDF.select($"zip".cast(IntegerType)))
    explained should include ("isnotnull(zip#")
  }
}
