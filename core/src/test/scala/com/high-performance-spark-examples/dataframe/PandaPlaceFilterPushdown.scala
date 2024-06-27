/**
 * Happy Panda Example for DataFrames.
 * Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types._

import com.highperformancespark.examples.dataframe.HappyPandas.PandaInfo
import com.highperformancespark.examples.dataframe.HappyPandas.Pandas
import com.holdenkarau.spark.testing._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class PandaPlaceFilterPushdown extends AnyFunSuite with DataFrameSuiteBase {

  override def appName: String = "pandaPlaceFilterPushdown"

  val basicList = List(
    ("a", "b", 1, 2))

  test("simpleFilterTest") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(basicList)
    val switched = inputDF.as[PandaInfo]
    val filtered = switched.filter(_.city === "a")
    assert(filtered.count() === 1)
  }
}
