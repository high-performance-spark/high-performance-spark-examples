/**
 * Checks basic Dataset magics
 */
package com.highperformancespark.examples.dataframe

import com.highperformancespark.examples.dataframe.HappyPandas.{PandaInfo, Pandas}
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.Matchers._
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.util.Random

class MixedDatasetSuite extends FunSuite with DataFrameSuiteBase {

  val rawPandaList = List(
    RawPanda(10L, "94110", "giant", true, Array(1.0, 0.9)),
    RawPanda(11L, "94110", "red", true, Array(1.0, 0.9)))

  val mixedDS = new MixedDataset(sqlContext)

  test("happy panda sums") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(rawPandaList)
    val inputDS = inputDF.as[RawPanda]
    val result = mixedDS.happyPandaSums(inputDS)
    assert(result === (2.0 +- 0.001))
  }

}
