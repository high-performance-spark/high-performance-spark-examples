package com.highperformancespark.examples.mllib

import com.highperformancespark.examples.dataframe._

import scala.collection.{Map, mutable}
import scala.collection.mutable.{ArrayBuffer, MutableList}

import org.apache.spark._
import org.apache.spark.rdd.RDD
//tag::imports[]
import org.apache.spark.mllib.linalg.Vectors
// Rename Vector to SparkVector to avoid conflicts with Scala's Vector class
import org.apache.spark.mllib.linalg.{Vector => SparkVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature._
//end::imports[]

class GoldilocksMLlib(sc: SparkContext) {

  def booleanToDouble(boolean: Boolean): Double = {
    if (boolean) 1.0 else 0.0
  }

  def toLabeledPointDense(rdd: RDD[RawPanda]): RDD[LabeledPoint] = {
    rdd.map(rp =>
      LabeledPoint(booleanToDouble(rp.happy),
        Vectors.dense(rp.attributes)))
  }

  def selectTopTenFeatures(rdd: RDD[LabeledPoint]): RDD[SparkVector] = {
    val selector = new ChiSqSelector(10)
    val model = selector.fit(rdd)
    val topFeatures = model.selectedFeatures
    val vecs = rdd.map(_.features)
    model.transform(vecs)
  }

  def selectAndKeepLabeled(rdd: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val selector = new ChiSqSelector(10)
    val model = selector.fit(rdd)
    rdd.map{
      case LabeledPoint(label, features) =>
        LabeledPoint(label, model.transform(features))
    }
  }

  //tag::hashingTFSimple[]
  def hashingTf(rdd: RDD[String]): RDD[SparkVector] = {
    val ht = new HashingTF()
    val tokenized = rdd.map(_.split(" ").toIterable)
    ht.transform(tokenized)
  }
  //end::hashingTFSimple[]

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

  def toLabeledPointWithHashing(rdd: RDD[RawPanda]): RDD[LabeledPoint] = {
    val ht = new HashingTF()
    rdd.map{rp =>
      val hashingVec = ht.transform(rp.pt)
      val combined = hashingVec.toArray ++ rp.attributes
      LabeledPoint(booleanToDouble(rp.happy),
        Vectors.dense(combined))
    }
  }

  def trainModel(rdd: RDD[LabeledPoint]) = {
  }

  def predict(rdd: RDD[SparkVector]) = {
  }

  def saveToPMML() = {
  }
}
