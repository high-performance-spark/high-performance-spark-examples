package com.highperformancespark.examples.mllib

import scala.collection.Map

import org.apache.spark._
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Vector => SparkVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import com.highperformancespark.examples.dataframe._
//end::imports[]

object GoldilocksMLlib {

  def booleanToDouble(boolean: Boolean): Double = {
    if (boolean) 1.0 else 0.0
  }

  def toLabeledPointDense(rdd: RDD[RawPanda]): RDD[LabeledPoint] = {
    //tag::toLabeledPointDense[]
    rdd.map(rp =>
      LabeledPoint(booleanToDouble(rp.happy),
        Vectors.dense(rp.attributes)))
    //end::toLabeledPointDense[]
  }

  //tag::toSparkVectorDense[]
  def toSparkVectorDense(input: Array[Double]) = {
    Vectors.dense(input)
  }
  //end::toSparkVectorDense[]

  //tag::selectTopTen[]
  def selectTopTenFeatures(rdd: RDD[LabeledPoint]):
      (ChiSqSelectorModel, Array[Int], RDD[SparkVector]) = {
    val selector = new ChiSqSelector(10)
    val model = selector.fit(rdd)
    val topFeatures = model.selectedFeatures
    val vecs = rdd.map(_.features)
    (model, topFeatures, model.transform(vecs))
  }
  //end::selectTopTen[]

  //tag::keepLabeled[]
  def selectAndKeepLabeled(rdd: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val selector = new ChiSqSelector(10)
    val model = selector.fit(rdd)
    rdd.map{
      case LabeledPoint(label, features) =>
        LabeledPoint(label, model.transform(features))
    }
  }
  //end::keepLabeled[]

  //tag::createLabelLookup[]
  def createLabelLookup[T](rdd: RDD[T]): Map[T, Double] = {
    val distinctLabels: Array[T] = rdd.distinct().collect()
    distinctLabels.zipWithIndex
      .map{case (label, x) => (label, x.toDouble)}.toMap
  }
  //end::createLabelLookup[]


  //tag::hashingTFSimple[]
  def hashingTf(rdd: RDD[String]): RDD[SparkVector] = {
    val ht = new HashingTF()
    val tokenized = rdd.map(_.split(" ").toIterable)
    ht.transform(tokenized)
  }
  //end::hashingTFSimple[]

  //tag::word2vecTrain[]
  def word2vecTrain(rdd: RDD[String]): Word2VecModel = {
    // Tokenize our data
    val tokenized = rdd.map(_.split(" ").toIterable)
    // Construct our word2vec model
    val wv = new Word2Vec()
    wv.fit(tokenized)
  }
  //end::word2vecTrain[]


  //tag::trainScaler[]
  // Trains a feature scaler and returns the scaler and scaled features
  def trainScaler(rdd: RDD[SparkVector]): (StandardScalerModel, RDD[SparkVector]) = {
    val scaler = new StandardScaler()
    val scalerModel = scaler.fit(rdd)
    (scalerModel, scalerModel.transform(rdd))
  }
  //end::trainScaler[]

  //tag::hashingTFPreserve[]
  def toVectorPerserving(rdd: RDD[RawPanda]): RDD[(RawPanda, SparkVector)] = {
    val ht = new HashingTF()
    rdd.map{panda =>
      val textField = panda.pt
      val tokenizedTextField = textField.split(" ").toIterable
      (panda, ht.transform(tokenizedTextField))
    }
  }
  //end::hashingTFPreserve[]

  //tag::hashingTFPreserveZip[]
  def hashingTFPreserveZip(rdd: RDD[RawPanda]): RDD[(RawPanda, SparkVector)] = {
    val ht = new HashingTF()
    val tokenized = rdd.map{panda => panda.pt.split(" ").toIterable}
    val vecs = ht.transform(tokenized)
    rdd.zip(vecs)
  }
  //end::hashingTFPreserveZip[]

  //tag::toLabeledPointWithHashing[]
  def toLabeledPointWithHashing(rdd: RDD[RawPanda]): RDD[LabeledPoint] = {
    val ht = new HashingTF()
    rdd.map{rp =>
      val hashingVec = ht.transform(rp.pt)
      val combined = hashingVec.toArray ++ rp.attributes
      LabeledPoint(booleanToDouble(rp.happy),
        Vectors.dense(combined))
    }
  }
  //end::toLabeledPointWithHashing[]

  //tag::train[]
  def trainModel(rdd: RDD[LabeledPoint]): LogisticRegressionModel = {
    val lr = new LogisticRegressionWithLBFGS()
    val lrModel = lr.run(rdd)
    lrModel
  }
  //end::train[]

  //tag::trainWithIntercept[]
  def trainModelWithInterept(rdd: RDD[LabeledPoint]): LogisticRegressionModel = {
    val lr = new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    val lrModel = lr.run(rdd)
    lrModel
  }
  //end::trainWithIntercept[]

  //tag::predict[]
  def predict(model: LogisticRegressionModel, rdd: RDD[SparkVector]): RDD[Double] = {
    model.predict(rdd)
  }
  //end::predict[]

  //tag::save[]
  def save(sc: SparkContext, path: String, model: LogisticRegressionModel) = {
    //tag::savePMML[]
    // Save to PMML - remote path
    model.toPMML(sc, path + "/pmml")
    // Save to PMML local path
    model.toPMML(path + "/pmml")
    //end::savePMML[]
    //tag::saveInternal[]
    // Save to internal - remote path
    model.save(sc, path + "/internal")
    //end::saveInternal[]
  }
  //end::save[]

  //tag::load[]
  def load(sc: SparkContext, path: String): LogisticRegressionModel = {
    LogisticRegressionModel.load(sc, path + "/internal")
  }
  //end::load[]
}
