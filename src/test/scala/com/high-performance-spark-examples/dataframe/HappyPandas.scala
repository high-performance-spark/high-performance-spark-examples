/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import com.highperformancespark.examples.dataframe.HappyPanda.PandaInfo
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.Matchers._

class HappyPandasTest extends DataFrameSuiteBase {
  val toronto = "toronto"
  val sandiego = "san diego"
  val virginia = "virginia"
  val pandInfoList = List(PandaInfo(toronto, "giant", 1, 2),
                          PandaInfo(sandiego, "red", 2, 3),
                          PandaInfo(virginia, "black", 1, 10))

  //tag::approxEqualDataFrames[]

  test("verify simple happy pandas Percentage") {
    val expectedResult = List(Row(toronto, 0.5), Row(sandiego, 2/3.0), Row(virginia, 1/10.0))
    val expectedDf = createDF(expectedResult, ("place", StringType),
                                              ("percentHappy", DoubleType))

    val inputDF = sqlContext.createDataFrame(pandInfoList)
    val result = HappyPanda.happyPandasPercentage(inputDF)

    approxEqualDataFrames(expectedDf, result, 1E-5)
  }
  //end::approxEqualDataFrames[]

  test("verify approx by hand") {
    val inputDF = sqlContext.createDataFrame(pandInfoList)
    val resultDF = HappyPanda.happyPandasPercentage(inputDF)
    val resultRows = resultDF.collect()

    val expectedRows = List(Row(toronto, 0.5), Row(sandiego, 2/3.0), Row(virginia, 1/10.0))

    //tag::approxEqualRow[]
    assert(expectedRows.size === resultRows.size)
    expectedRows.zip(resultRows).foreach{case (r1, r2) =>
      assert(r1(0) === r2(0))
      assert(r1.getDouble(1) === (r2.getDouble(1) +- 0.001))
    }
    //end::approxEqualRow[]
  }

  test("test encode Panda type") {
    val inputDF = sqlContext.createDataFrame(pandInfoList)
    val resultDF = HappyPanda.encodePandaType(inputDF)

    val expectedRows = List(Row(toronto, 0), Row(sandiego, 1), Row(virginia, 2))
    val expectedDF = createDF3(expectedRows, ("place", StringType, true),
                                             ("encodedType", IntegerType, false))

    equalDataFrames(expectedDF, resultDF)
  }

  //tag::exactEqualDataFrames[]
  test("verify exact equality") {
    // test minHappyPandas
    val inputDF = sqlContext.createDataFrame(pandInfoList)
    val result = HappyPanda.minHappyPandas(inputDF, 2)
    val resultRows = result.collect()

    val expectedRows = List(Row(sandiego, "red", 2, 3))
    assert(expectedRows === resultRows)
  }
  //end::exactEqualDataFrames[]

  test("test happyPandasPlaces") {
    val inputDF = sqlContext.createDataFrame(pandInfoList)
    val resultDF = HappyPanda.happyPandasPlaces(inputDF)

    val expectedRows = List(PandaInfo(toronto, "giant", 1, 2),
                            PandaInfo(sandiego, "red", 2, 3))
    val expectedDF = sqlContext.createDataFrame(expectedRows)

    equalDataFrames(expectedDF, resultDF)
  }

  // Make a test once we have hivectx in the base
  def futureTestRrelativePandaSize() {
    val sqlCtx = sqlContext
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


  private def createDF(list: List[Row], fields: Tuple2[String, DataType]*) =
    sqlContext.createDataFrame(sc.parallelize(list), structType2(fields))

  private def structType2(fields: Seq[(String, DataType)]) =
    StructType(fields.map(f => (StructField(f._1, f._2))).toList)


  private def createDF3(list: List[Row], fields: Tuple3[String, DataType, Boolean]*) =
    sqlContext.createDataFrame(sc.parallelize(list), structType3(fields))

  private def structType3(fields: Seq[(String, DataType, Boolean)]) =
    StructType(fields.map(f => (StructField(f._1, f._2, f._3))).toList)
}