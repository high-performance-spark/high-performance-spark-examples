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
//tag::basicImport[]
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
//end::basicImport[]
//tag::renameImport[]
import org.apache.spark.ml.linalg.{Vector => SparkVector}
//end::renameImport[]
import org.apache.spark.ml.param._
import org.apache.spark.ml.tuning._

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
    //tag::vectorAssembler[]
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
    //end::stringIndexer[]
  }

  def reverseStringIndexer(sbModel: StringIndexerModel) = {
    //tag::indexToString[]
    // Construct the inverse of the model to go from index-to-string
    // after prediction.
    val sbInverse = new IndexToString()
    sbInverse.setInputCol("prediction")
    sbInverse.setLabels(sbModel.labels)
    //end::indexToString[]
    // Or if meta data is present
    //tag::indexToStringMD[]
    // Construct the inverse of the model to go from
    // index-to-string after prediction.
    val sbInverseMD = new IndexToString()
    sbInverseMD.setInputCol("prediction")
    //end::indexToStringMD[]
  }

  def normalizer() = {
    //tag::normalizer[]
    val normalizer = new Normalizer()
    normalizer.setInputCol("features")
    normalizer.setOutputCol("normalized_features")
    //end::normalizer[]
  }

  def paramSearch(df: DataFrame) = {
    val tokenizer = new Tokenizer()
    tokenizer.setInputCol("name")
    tokenizer.setOutputCol("tokenized_name")
    val hashingTF = new HashingTF()
    hashingTF.setInputCol("tokenized_name")
    hashingTF.setOutputCol("name_tf")
    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("size", "zipcode", "name_tf",
      "attributes"))
    val normalizer = new Normalizer()
    normalizer.setInputCol("features")
    normalizer.setOutputCol("normalized_features")
    val nb = new NaiveBayes()
    nb.setLabelCol("happy")
    nb.setFeaturesCol("normalized_features")
    nb.setPredictionCol("prediction")
    val pipeline = new Pipeline()
    pipeline.setStages(Array(tokenizer, hashingTF, assembler, normalizer, nb))
    //tag::createSimpleParamGrid[]
    // ParamGridBuilder constructs an Array of parameter combinations.
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(nb.smoothing, Array(0.1, 0.5, 1.0, 2.0))
      .build()
    //end::createSimpleParamGrid[]
    //tag::runSimpleCVSearch[]
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
    val cvModel = cv.fit(df)
    val bestModel = cvModel.bestModel
    //end::runSimpleCVSearch[]
    //tag::complexParamSearch[]
    val complexParamGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(nb.smoothing, Array(0.1, 0.5, 1.0, 2.0))
      .addGrid(hashingTF.numFeatures, Array(1 << 18, 1 << 20))
      .addGrid(hashingTF.binary, Array(true, false))
      .addGrid(normalizer.p, Array(1.0, 1.5, 2.0))
      .build()
    //end::complexParamSearch[]
    bestModel
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
