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
    RawPanda(10L, "94110", "giant", true, Array(1.0, 0.9, 20.0)),
    RawPanda(11L, "94110", "red", true, Array(1.0, 0.7, 30.0)))

  test("happy panda sums") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mixedDS = new MixedDataset(sqlCtx)
    val inputDF = sqlCtx.createDataFrame(rawPandaList)
    val inputDS = inputDF.as[RawPanda]
    val result = mixedDS.happyPandaSums(inputDS)
    assert(result === (2.0 +- 0.001))
  }

  test("basic select") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(rawPandaList)
    val inputDS = inputDF.as[RawPanda]
    val mixedDS = new MixedDataset(sqlCtx)
    val squishy = mixedDS.squishyPandas(inputDS).collect()
    assert(squishy(0)._2 == true)
  }

  test("funquery") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(rawPandaList)
    val inputDS = inputDF.as[RawPanda]
    val mixedDS = new MixedDataset(sqlCtx)
    val summedAttrs = mixedDS.funMap(inputDS).collect()
    assert(summedAttrs(0) === 21.9 +- 0.001)
    assert(summedAttrs(1) === 31.7 +- 0.001)
  }

  test("max pandas size per zip") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(rawPandaList)
    val inputDS = inputDF.as[RawPanda]
    val mixedDS = new MixedDataset(sqlCtx)
    val bigPandas = mixedDS.maxPandaSizePerZip(inputDS).collect()
    assert(bigPandas.size === 1)
    assert(bigPandas(0)._2 === 30.0 +- 0.00001)
  }
}
