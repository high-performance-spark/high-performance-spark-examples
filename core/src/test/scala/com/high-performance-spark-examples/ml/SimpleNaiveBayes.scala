/**
 * Simple tests for our SimpleNaiveBayes demo pipeline stage
 */
package com.highperformancespark.examples.ml

import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

import com.highperformancespark.examples.dataframe.HappyPandas.PandaInfo
import com.highperformancespark.examples.dataframe.HappyPandas.Pandas
import com.holdenkarau.spark.testing._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

case class MiniPanda(happy: Double, fuzzy: Double, old: Double)

class SimpleNaiveBayesSuite extends AnyFunSuite with DataFrameSuiteBase {
  val miniPandasList = List(
    MiniPanda(1.0, 1.0, 1.0),
    MiniPanda(1.0, 1.0, 0.0),
    MiniPanda(1.0, 1.0, 0.0),
    MiniPanda(0.0, 0.0, 1.0),
    MiniPanda(0.0, 0.0, 0.0))

  test("simple sanity test") {
    val session = spark
    import session.implicits._
    val ds: Dataset[MiniPanda] = session.createDataset(miniPandasList)
    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("fuzzy", "old"))
    assembler.setOutputCol("magical_features")
    val snb = new SimpleNaiveBayes()
    snb.setLabelCol("happy")
    snb.setFeaturesCol("magical_features")
    val pipeline = new Pipeline().setStages(Array(assembler, snb))
    val model = pipeline.fit(ds)
    val test = ds.select("fuzzy", "old")
    val predicted = model.transform(test)
    assert(predicted.count() === miniPandasList.size)
    val nbModel = model.stages(1).asInstanceOf[SimpleNaiveBayesModel]
    assert(nbModel.getFeaturesCol === "magical_features")
    assert(nbModel.copy(ParamMap.empty).getFeaturesCol === "magical_features")
  }
}
