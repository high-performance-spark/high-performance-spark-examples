/**
 * Simple tests for DStreamSuite - normally we would use streaming tests but since we want to test
 * context creation as well we don't
 */
package com.highperformancespark.examples.streaming

import org.apache.spark.streaming._

import java.lang.Thread
import com.holdenkarau.spark.testing._

import org.scalatest.FunSuite

class DStreamExamplesSuite extends FunSuite with SharedSparkContext {
  test("simple set up") {
    val ssc = DStreamExamples.makeStreamingContext(sc)
    val inputStream = DStreamExamples.fileAPIExample(ssc, "./")
    val repartitioned = DStreamExamples.repartition(inputStream)
    repartitioned.foreachRDD(rdd => assert(rdd.partitioner.get.numPartitions == 20))
    ssc.start()
    // This is bad don't do this - but we don't have the full test tools here
    Thread.sleep(100)
    ssc.stop()
  }
}
