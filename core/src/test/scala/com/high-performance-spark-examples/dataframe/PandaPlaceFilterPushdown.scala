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

case class ExtraMagic(
  place: String,
  pandaType: String,
  happyPandas: Integer,
  totalPandas: Integer,
  extraInfo: Integer)


class PandaPlaceFilterPushdown extends AnyFunSuite with DataFrameSuiteBase {

  override def appName: String = "pandaPlaceFilterPushdown"

  val basicList = List(
    ExtraMagic("a", "b", 1, 2, 3),
    ExtraMagic("toronto", "b", 1, 2, 3),
  )

  test("simpleFilterTest") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(basicList)
    val restrictedDF = inputDF.select($"place", $"pandaType", $"happyPandas", $"totalPandas")
    val switched = inputDF.as[PandaInfo]
    // Note if we write the filter with functional syntax it does not push down.
    val filtered = switched.filter($"place" === "a")
    assert(filtered.count() === 1)
  }
}
