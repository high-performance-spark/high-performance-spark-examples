/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
package com.highperformancespark.examples.dataframe

import com.highperformancespark.examples.dataframe.HappyPanda.{PandaInfo, Pandas}
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.util.Random

class HappyPandasTest extends DataFrameSuiteBase {
  val toronto = "toronto"
  val sandiego = "san diego"
  val virginia = "virginia"
  val pandaInfoList = List(PandaInfo(toronto, "giant", 1, 2),
                          PandaInfo(sandiego, "red", 2, 3),
                          PandaInfo(virginia, "black", 1, 10))

  val pandasList = List(Pandas("bata", "10010", 10, 2),
                        Pandas("wiza", "10010", 20, 4),
                        Pandas("dabdob", "11000", 8, 2),
                        Pandas("hanafy", "11000", 15, 7),
                        Pandas("hamdi", "11111", 20, 10))

  //tag::approxEqualDataFrames[]

  test("verify simple happy pandas Percentage") {
    val expectedResult = List(Row(toronto, 0.5), Row(sandiego, 2/3.0), Row(virginia, 1/10.0))
    val expectedDf = createDF(expectedResult, ("place", StringType),
                                              ("percentHappy", DoubleType))

    val inputDF = sqlContext.createDataFrame(pandaInfoList)
    val result = HappyPanda.happyPandasPercentage(inputDF)

    approxEqualDataFrames(expectedDf, result, 1E-5)
  }
  //end::approxEqualDataFrames[]

  test("verify approx by hand") {
    val inputDF = sqlContext.createDataFrame(pandaInfoList)
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
    val inputDF = sqlContext.createDataFrame(pandaInfoList)
    val resultDF = HappyPanda.encodePandaType(inputDF)

    val expectedRows = List(Row(toronto, 0), Row(sandiego, 1), Row(virginia, 2))
    val expectedDF = createDF3(expectedRows, ("place", StringType, true),
                                             ("encodedType", IntegerType, false))

    equalDataFrames(expectedDF, resultDF)
  }

  //tag::exactEqualDataFrames[]
  test("verify exact equality") {
    // test minHappyPandas
    val inputDF = sqlContext.createDataFrame(pandaInfoList)
    val result = HappyPanda.minHappyPandas(inputDF, 2)
    val resultRows = result.collect()

    val expectedRows = List(Row(sandiego, "red", 2, 3))
    assert(expectedRows === resultRows)
  }
  //end::exactEqualDataFrames[]

  test("test happyPandasPlaces") {
    val inputDF = sqlContext.createDataFrame(pandaInfoList)
    val resultDF = HappyPanda.happyPandasPlaces(inputDF)

    val expectedRows = List(PandaInfo(toronto, "giant", 1, 2),
                            PandaInfo(sandiego, "red", 2, 3))
    val expectedDF = sqlContext.createDataFrame(expectedRows)

    equalDataFrames(expectedDF, resultDF)
  }

  test("test maxPandaSizePerZip") {
    val inputDF = sqlContext.createDataFrame(pandasList)
    val resultDF = HappyPanda.maxPandaSizePerZip(inputDF)

    val expectedRows = List(Row(pandasList(1).zip, pandasList(1).pandaSize),
                            Row(pandasList(3).zip, pandasList(3).pandaSize),
                            Row(pandasList(4).zip, pandasList(4).pandaSize))
    val expectedDF = createDF(expectedRows, ("zip", StringType),
                                            ("max(pandaSize)", IntegerType))

    equalDataFrames(expectedDF.orderBy("zip"), resultDF.orderBy("zip"))
  }

  test("test minMaxPandaSizePerZip"){
    val inputDF = sqlContext.createDataFrame(pandasList)
    val resultDF = HappyPanda.minMaxPandaSizePerZip(inputDF)

    val expectedRows = List(
      Row(pandasList(1).zip, pandasList(0).pandaSize, pandasList(1).pandaSize),
      Row(pandasList(3).zip, pandasList(2).pandaSize, pandasList(3).pandaSize),
      Row(pandasList(4).zip, pandasList(4).pandaSize, pandasList(4).pandaSize))

    val expectedDF = createDF(expectedRows, ("zip", StringType),
                                            ("min(pandaSize)", IntegerType),
                                            ("max(pandaSize)", IntegerType))

    equalDataFrames(expectedDF.orderBy("zip"), resultDF.orderBy("zip"))
  }

  test("test minPandaSizeMaxAgePerZip") {
    val inputDF = sqlContext.createDataFrame(pandasList)
    val resultDF = HappyPanda.minPandaSizeMaxAgePerZip(inputDF)

    val expectedRows = List(
      Row(pandasList(1).zip, pandasList(0).pandaSize, pandasList(1).age),
      Row(pandasList(3).zip, pandasList(2).pandaSize, pandasList(3).age),
      Row(pandasList(4).zip, pandasList(4).pandaSize, pandasList(4).age))

    val expectedDF = createDF(expectedRows, ("zip", StringType),
                                            ("min(pandaSize)", IntegerType),
                                            ("max(age)", IntegerType))

    equalDataFrames(expectedDF.orderBy("zip"), resultDF.orderBy("zip"))
  }

  test("test complexAggPerZip") {
    val inputDF = sqlContext.createDataFrame(pandasList)
    val resultDF = HappyPanda.minMeanSizePerZip(inputDF)

    val expectedRows = List(
      Row(pandasList(1).zip, pandasList(0).pandaSize, 15.0),
      Row(pandasList(3).zip, pandasList(2).pandaSize, 11.5),
      Row(pandasList(4).zip, pandasList(4).pandaSize, 20.0))

    val expectedDF = createDF(expectedRows, ("zip", StringType),
                                            ("min(pandaSize)", IntegerType),
                                            ("avg(pandaSize)", DoubleType))

    approxEqualDataFrames(expectedDF.orderBy("zip"), resultDF.orderBy("zip"), 1e-5)
  }


  test("test Simple SQL example") {
    val inputDF = sqlContext.createDataFrame(pandasList)
    val resultDF = HappyPanda.simpleSqlExample(inputDF)

    val expectedRows = List(pandasList(0), pandasList(2))
    val expectedDF = sqlContext.createDataFrame(expectedRows)

    equalDataFrames(expectedDF, resultDF)
  }

  test("test Order Pandas") {
    val inputDF = sqlContext.createDataFrame(pandasList)
    val resultDF = HappyPanda.orderPandas(inputDF)

    val expectedRows = List(pandasList(2), pandasList(0), pandasList(3),
                            pandasList(4), pandasList(1))
    val expectedDF = sqlContext.createDataFrame(expectedRows)

    equalDataFrames(expectedDF, resultDF)
  }


  test("test computeRelativePandaSizes") {
    val inputPandaList = loadPandaStuffies()
    val inputDF = sqlContext.createDataFrame(inputPandaList)

    val resultDF = HappyPanda.computeRelativePandaSizes(inputDF)

    val expectedDF = getExpectedPandasRelativeSize(inputPandaList, -10, 10)

    approxEqualDataFrames(expectedDF.orderBy("name"), resultDF.orderBy("name"), 1e-5)
  }

  private def getExpectedPandasRelativeSize(pandaList: List[Pandas], start: Int, end: Int):DataFrame = {

    val expectedRows =
      pandaList
        .groupBy(_.zip)
        .map(zipPandas => (zipPandas._1, zipPandas._2.sortBy(_.age)))
        .flatMap(zipPandas => {
          val pandas = zipPandas._2
          val length = pandas.size - 1
          val result = new mutable.MutableList[Row]

          for (i <- 0 to length) {
            var totalSum = 0
            val startOffset = math.max(0, i + start)
            val endOffset = math.min(length, i + end)

            for (j <- (startOffset to endOffset))
              totalSum += pandas(j).pandaSize

            val count = (endOffset - startOffset + 1)
            val average = totalSum.toDouble / count

            val panda = pandas(i)
            result += Row(panda.name, panda.zip, panda.pandaSize, panda.age, panda.pandaSize - average)
          }

          result
        }).toList

    val expectedDF = createDF(expectedRows, ("name", StringType),
                                            ("zip", StringType),
                                            ("pandaSize", IntegerType),
                                            ("age", IntegerType),
                                            ("panda_relative_size", DoubleType))

    expectedDF
  }

  private def loadPandaStuffies(): List[Pandas] = {
    val zipCount = 20
    val maxPandasPerZip = 100
    val maxPandaAge = 50
    val maxPandaSize = 500
    val random = new Random()

    val pandas =
      (1 to zipCount)
      .flatMap(zipId => {
        val pandasCount = 1 + random.nextInt(maxPandasPerZip)
        val zipName = s"zip($zipId)"

        (1 to pandasCount).map(pandaId => {
          val name = s"panda($pandaId)($zipId)"
          val size = 1 + random.nextInt(maxPandaSize)
          val age = 1 + random.nextInt(maxPandaAge)

           Pandas(name, zipName, size, age)
        }
      )

    })

    pandas.toList
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