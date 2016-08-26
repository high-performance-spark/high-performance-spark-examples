package com.highperformancespark.examples.ml

import com.highperformancespark.examples.dataframe._

import scala.collection.{Map, mutable}
import scala.collection.mutable.{ArrayBuffer, MutableList}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.linalg._

object SimplePipeline {
  def constructAndSetParams(df: DataFrame) = {
    val sqlCtx = df.sqlContext
    //tag::constructSetParams[]
    val hashingTF = new HashingTF()
    hashingTF.setInputCol("input")
    hashingTF.setOutputCol("hashed_terms")
    //end::constructSetParams[]
  }

  def constructSimpleTransformer(df: DataFrame) = {
    val sqlCtx = df.sqlContext
    //tag::simpleTransformer[]
    val hashingTF = new HashingTF()
    // We don't set the output column here so the default output column of
    // uid + "__output" is used.
    hashingTF.setInputCol("input")
    // Transformer the input
    val transformed = hashingTF.transform(df)
    // Since we don't know what the uid is we can use the getOutputCol function
    val outputCol = hashingTF.getOutputCol
    //end::simpleTransformer[]
    (outputCol, transformed)
  }

  def constructVectorAssembler() = {
    //tag::vectorAsssembler[]
    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("size", "zipcode"))
    //end::vectorAssembler[]
  }

  // Here is a simple tokenizer to hashingtf transformer manually chained
  def simpleTokenizerToHashing(df: DataFrame) = {
    //tag::simpleTokenizerToHashing[]
    val tokenizer = new Tokenizer()
    tokenizer.setInputCol("name")
    tokenizer.setOutputCol("tokenized_name")
    val tokenizedData = tokenizer.transform(df)
    val hashingTF = new HashingTF()
    hashingTF.setInputCol("tokenized_name")
    hashingTF.setOutputCol("name_tf")
    hashingTF.transform(tokenizedData)
    //end::simpleTokenizerToHashing[]
  }

  def constructSimpleEstimator(df: DataFrame) = {
    val sqlCtx = df.sqlContext
    //tag::simpleNaiveBayes[]
    val nb = new NaiveBayes()
    nb.setLabelCol("happy")
    nb.setFeaturesCol("features")
    nb.setPredictionCol("prediction")
    val nbModel = nb.fit(df)
    //end::simpleNaiveBayes[]
  }

  def stringIndexer(df: DataFrame) = {
    //tag::stringIndexer[]
    // Construct a simple string indexer
    val sb = new StringIndexer()
    sb.setInputCol("name")
    sb.setOutputCol("indexed_name")
    // Construct the model based on the input
    val sbModel = sb.fit(df)
    // Construct the inverse of the model to go from index-to-string after prediction.
    val sbInverse = new IndexToString()
    sbInverse.setInputCol("prediction")
    sbInverse.setLabels(sbModel.labels)
    //end::stringIndexer[]
  }

  def buildSimplePipeline(df: DataFrame) = {
    //tag::simplePipeline[]
    val tokenizer = new Tokenizer()
    tokenizer.setInputCol("name")
    tokenizer.setOutputCol("tokenized_name")
    val hashingTF = new HashingTF()
    hashingTF.setInputCol("tokenized_name")
    hashingTF.setOutputCol("name_tf")
    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("size", "zipcode", "name_tf",
      "attributes"))
    val nb = new NaiveBayes()
    nb.setLabelCol("happy")
    nb.setFeaturesCol("features")
    nb.setPredictionCol("prediction")
    val pipeline = new Pipeline()
    pipeline.setStages(Array(tokenizer, hashingTF, assembler, nb))
    //end::simplePipeline[]
    //tag::trainPipeline[]
    val pipelineModel = pipeline.fit(df)
    //end::trainPipeline[]
    //tag::accessStages[]
    val tokenizer2 = pipelineModel.stages(0).asInstanceOf[Tokenizer]
    val nbFit = pipelineModel.stages.last.asInstanceOf[NaiveBayesModel]
    //end::accessStages[]
    //tag::newPipeline[]
    val normalizer = new Normalizer()
    normalizer.setInputCol("features")
    normalizer.setOutputCol("normalized_features")
    nb.setFeaturesCol("normalized_features")
    pipeline.setStages(Array(tokenizer, hashingTF, assembler, normalizer, nb))
    val normalizedPipelineModel = pipelineModel.transform(df)
    //end::newPipeline[]
  }
}
