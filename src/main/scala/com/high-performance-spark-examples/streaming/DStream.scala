/**
 * Streaming Pandas Example with the old DStream APIs.
 */
package com.highperformancespark.examples.streaming

import scala.reflect.ClassTag

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark._
import org.apache.spark.rdd.RDD

//tag::DStreamImports[]
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
//end::DStreamImports[]

object DStreamExamples {
  def makeStreamingContext(sc: SparkContext) = {
    //tag::ssc[]
    val batchInterval = Seconds(1)
    new StreamingContext(sc, batchInterval)
    //end::ssc[]
  }

  def makeRecoverableStreamingContext(sc: SparkContext, checkpointDir: String) = {
    //tag::sscRecover[]
    def createStreamingContext(): StreamingContext = {
      val batchInterval = Seconds(1)
      val ssc = new StreamingContext(sc, batchInterval)
      ssc.checkpoint(checkpointDir)
      // Then create whatever stream is required
      // And whatever mappings need to go on those streams
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointDir,
      createStreamingContext _)
    // Do whatever work needs to be done regardless of state
    // Start context and run
    ssc.start()
    //end::sscRecover[]
  }

  def fileAPIExample(ssc: StreamingContext, path: String):
      DStream[(Long, String)] = {
    //tag::file[]
    // You don't need to write the types of the InputDStream but it for illustration
    val inputDStream: InputDStream[(LongWritable, Text)] =
      ssc.fileStream[LongWritable, Text, TextInputFormat](path)
    // Convert the hadoop types to native JVM types for simplicity
    def convert(input: (LongWritable, Text)) = {
      (input._1.get(), input._2.toString())
    }
    val input: DStream[(Long, String)] = inputDStream.map(convert)
    //end::file[]
    input
  }

  def repartition(dstream: DStream[_]) = {
    //tag::repartition[]
    dstream.repartition(20)
    //end::repartition[]
  }

  //tag::repartitionWithTransform[]
  def dStreamRepartition[A: ClassTag](dstream: DStream[A]): DStream[A] = {
    dstream.transform{rdd => rdd.repartition(20)}
  }
  //end::repartitionWithTransform[]

  def simpleTextOut(target: String, dstream: DStream[_]) = {
    //tag::simpleOut[]
    dstream.saveAsTextFiles(target)
    //end::simpleOut[]
  }

  def foreachSaveSequence(target: String, dstream: DStream[(Long, String)]) = {
    //tag::foreachSave[]
    dstream.foreachRDD{(rdd, window) =>
      rdd.saveAsSequenceFile(target + window)
    }
    //end::foreachSave[]
  }
}
