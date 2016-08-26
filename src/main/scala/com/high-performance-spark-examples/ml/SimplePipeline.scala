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
    //end::simpleNaiveBayes[]
  }

  def trainSimplePipeline(df: DataFrame) = {
    val sqlCtx = df.sqlContext
  }
}
