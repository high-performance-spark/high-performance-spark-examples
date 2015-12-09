/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types._

import com.holdenkarau.spark.testing._

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.exceptions.TestFailedException

class HappyPandasTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {
  val inputList = List(PandaInfo("toronto", 2, 1), PandaInfo("san diego", 3, 2))

  //tag::approxEqualDataFrames[]
  test("verify simple happy pandas") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val expectedResult = List(Row("toronto", 0.5), Row("san diego", 2/3.0))
    val expectedDf = sqlCtx.createDataFrame(sc.parallelize(expectedResult),
      StructType(List(StructField("place", StringType),
        StructField("percentHappy", DoubleType))))
    val inputDF = sqlCtx.createDataFrame(inputList)
    val result = HappyPanda.happyPandas(inputDF)
    approxEqualDataFrames(expectedDf, result, 1E-5)
  }
  //end::approxEqualDataFrames[]

  test("verify approx by hand") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val expectedRows = List(Row("toronto", 0.5), Row("san diego", 2/3.0))
    val inputDF = sqlCtx.createDataFrame(inputList)
    val result = HappyPanda.happyPandas(inputDF)
    val resultRows = result.collect()
    //tag::approxEqualRow[]
    assert(expectedRows.size === resultRows.size)
    expectedRows.zip(resultRows).foreach{case (r1, r2) =>
      assert(r1(0) === r2(0))
      assert(r1.getDouble(1) === (r2.getDouble(1) +- 0.001))
    }
    //end::approxEqualRow[]
  }

  //tag::exactEqualDataFrames[]
  test("verify exact equality") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(inputList)
    val result = HappyPanda.minHappyPandas(inputDF, 2)
    val resultRows = result.collect()
    assert(List(Row("san diego", 3, 2)) === resultRows)
  }
  //end::exactEqualDataFrames[]

  // Make a test once we have hivectx in the base
  def futureTestRrelativePandaSize() {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // TODO: Generate some data instead of using the small static data
    val inputDF = loadPandaStuffies(sqlCtx)
    val result = HappyPanda.computeRelativePandaSizes(inputDF)
    val resultRows = result.collect()
    assert(List() === resultRows)
  }

  def loadPandaStuffies(sqlCtx: SQLContext): DataFrame = {
    val pandaStuffies = List(Row("ikea", null, 0.2, 94110),
      Row("tube", 6, 0.4, 94110),
      Row("panda", 6, 0.5, 94110),
      Row("real", 30, 77.5, 100000))
    val schema = StructType(List(StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("pandaSize", DoubleType, true),
      StructField("zip", IntegerType, true)))
    sqlCtx.createDataFrame(sc.parallelize(pandaStuffies), schema)
  }
}

case class PandaInfo(place: String, totalPandas: Integer, happyPandas: Integer)
