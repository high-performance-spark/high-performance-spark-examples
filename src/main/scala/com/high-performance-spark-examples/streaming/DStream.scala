/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
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

class DStreamExamples(sc: SparkContext, ssc: StreamingContext) {
  def makeStreamingContext() = {
    //tag::ssc[]
    val batchInterval = Seconds(1)
    new StreamingContext(sc, batchInterval)
    //end::ssc[]
  }

  def makeRecoverableStreamingContext(checkpointDir: String) = {
    //tag::sscRecover[]
    def createStreamingContext(): StreamingContext = {
      val batchInterval = Seconds(1)
      val ssc = new StreamingContext(sc, batchInterval)
      ssc.checkpoint(checkpointDir)
      // Then create whatever stream are required
      // And whatever mappings need to go on those streams
      ssc
    }
    val context = StreamingContext.getOrCreate(checkpointDir,
      createStreamingContext _)
    // Do whatever work needs to be regardless of state
    // Start context and run
    ssc.start()
    //end::sscRecover[]
  }

  def fileAPIExample(path: String) = {
    //tag::file[]
    ssc.fileStream[LongWritable, Text, TextInputFormat](path)
    //end::file[]
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
