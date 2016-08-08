package com.highperformancespark.examples.mllib

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
//tag::extraImports[]
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
//end::extraImports[]

//tag::basicPipelineSetup[]
class HardCodedWordCountStage(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("hardcodedwordcount"))

  def copy(extra: ParamMap): HardCodedWordCountStage = {
    defaultCopy(extra)
  }
//end::basicPipelineSetup[]

  //tag::basicTransformSchema[]
  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex("happy_pandas")
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField("happy_panda_counts", IntegerType, false))
  }
  //end::basicTransformSchema[]

  //tag::transformFunction[]
  def transform(df: Dataset[_]): DataFrame = {
    val wordcount = udf { in: String => in.split(" ").size }
    df.select(col("*"),
      wordcount(df.col("happy_pandas")).as("happy_panda_counts"))
  }
  //end::transformFunction[]
}


//tag::paramTransformer[]
class ConfigurableWordCount(override val uid: String) extends Transformer {
  final val inputCol= new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
//end::paramTransformer[]

  def this() = this(Identifiable.randomUID("configurablewordcount"))

  def copy(extra: ParamMap): HardCodedWordCountStage = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  def transform(df: Dataset[_]): DataFrame = {
    val wordcount = udf { in: String => in.split(" ").size }
    df.select(col("*"), wordcount(df.col($(inputCol))).as($(outputCol)))
  }
}
