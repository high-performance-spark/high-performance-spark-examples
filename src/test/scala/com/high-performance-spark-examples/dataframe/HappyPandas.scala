/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import com.holdenkarau.spark.testing._

import org.scalatest.FunSuite
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

}

case class PandaInfo(place: String, totalPandas: Integer, happyPandas: Integer)
