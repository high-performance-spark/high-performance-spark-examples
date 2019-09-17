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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class MixedDatasetSuite extends FunSuite with DataFrameSuiteBase with DatasetSuiteBase with RDDComparisons {

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
    assert(squishy(0)._2 === true)
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

  test("max pandas size per zip scala version") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(rawPandaList)
    val inputDS = inputDF.as[RawPanda]
    val mixedDS = new MixedDataset(sqlCtx)
    val bigPandas = mixedDS.maxPandaSizePerZipScala(inputDS).collect()
    assert(bigPandas.size === 1)
    assert(bigPandas(0)._2 === 30.0 +- 0.00001)
  }

  test("union pandas") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val happyPandas = sqlCtx.createDataset(rawPandaList.take(1))
    val sadPandas = sqlCtx.createDataset(rawPandaList.drop(1))
    val mixedDS = new MixedDataset(sqlCtx)
    val unionPandas = mixedDS.unionPandas(happyPandas, sadPandas).collect
    assert(unionPandas.toSet == rawPandaList.toSet)
  }

  test("typed query") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(rawPandaList)
    val inputDS = inputDF.as[RawPanda]
    val mixedDS = new MixedDataset(sqlCtx)
    val typedResult = mixedDS.typedQueryExample(inputDS)
    assert(typedResult.collect().toList == rawPandaList.map(_.attributes(0)))
  }

  test("join different dataset") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val pandaDS = sqlCtx.createDataFrame(rawPandaList).as[RawPanda]
    val rawCoffeeShop = List(CoffeeShop("94110", "Starbucks"), CoffeeShop("98765", "Caribou"))
    val coffeeShopDS = sqlCtx.createDataFrame(rawCoffeeShop).as[CoffeeShop]
    val mixedDS = new MixedDataset(sqlCtx)
    val joinResult = mixedDS.joinSample(pandaDS, coffeeShopDS)
    val expected = for {
      panda <- rawPandaList
      coffeeShop <- rawCoffeeShop
      if (panda.zip == coffeeShop.zip)
    } yield (panda, coffeeShop)
    assert(joinResult.collect().toSet == expected.toSet)
  }

  test("self join") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val inputDF = sqlCtx.createDataFrame(rawPandaList)
    val inputDS = inputDF.as[RawPanda]
    val mixedDS = new MixedDataset(sqlCtx)
    val selfJoinResult = mixedDS.selfJoin(inputDS)
    val expected = for {
      left <- rawPandaList
      right <- rawPandaList 
      if (left.zip == right.zip)
    } yield (left, right)
    assert(selfJoinResult.collect().toSet == expected.toSet)
  }

  test("convert an RDD to DS") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mixedDS = new MixedDataset(sqlCtx)
    val rdd = sc.parallelize(rawPandaList)
    val result = mixedDS.fromRDD(rdd)
    val expected = sqlCtx.createDataFrame(rawPandaList).as[RawPanda]
    assertDatasetEquals(expected, result)
  }

  test("convert a Dataset to an RDD") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mixedDS = new MixedDataset(sqlCtx)
    val rdd = sc.parallelize(rawPandaList)  
    val dataset = sqlCtx.createDataFrame(rawPandaList).as[RawPanda]
    val result = mixedDS.toRDD(dataset)
    val expected = sc.parallelize(rawPandaList)
    assertRDDEquals(expected, result)
  }

  test("convert a Dataset to a DataFrame") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mixedDS = new MixedDataset(sqlCtx)
    val rdd = sc.parallelize(rawPandaList)  
    val dataset = sqlCtx.createDataFrame(rawPandaList).as[RawPanda]
    val result = mixedDS.toDF(dataset)
    val expected = sqlCtx.createDataFrame(rawPandaList)
    assertDataFrameEquals(expected, result)
  }


  test("convert a DataFrame to a DataSset") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mixedDS = new MixedDataset(sqlCtx)
    val rdd = sc.parallelize(rawPandaList)  
    val dataframe =  sqlCtx.createDataFrame(rawPandaList)
    val result = mixedDS.fromDF(dataframe)
    val expected = sqlCtx.createDataFrame(rawPandaList).as[RawPanda]
    assertDatasetEquals(expected, result)
  }

}
