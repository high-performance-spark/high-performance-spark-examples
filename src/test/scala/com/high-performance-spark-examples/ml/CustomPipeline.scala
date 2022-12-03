/**
  * Simple tests for our CustomPipeline demo pipeline stage
  */
package com.highperformancespark.examples.ml

import org.apache.spark.sql.Dataset

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

case class TestRow(id: Int, inputColumn: String)

class CustomPipelineSuite extends FunSuite with DataFrameSuiteBase {
  val d = List(
    TestRow(0, "a"),
    TestRow(1, "b"),
    TestRow(2, "c"),
    TestRow(3, "a"),
    TestRow(4, "a"),
    TestRow(5, "c")
  )

  test("test spark context") {
    val session = spark
    val rdd = session.sparkContext.parallelize(1 to 10)
    assert(rdd.sum === 55)
  }

  test("simple indexer test") {
    val session = spark
    import session.implicits._
    val ds: Dataset[TestRow] = session.createDataset(d)
    val indexer = new SimpleIndexer()
    indexer.setInputCol("inputColumn")
    indexer.setOutputCol("categoryIndex")
    val model = indexer.fit(ds)
    val predicted = model.transform(ds)
    assert(predicted.columns.contains("categoryIndex"))
    predicted.show()
  }
}