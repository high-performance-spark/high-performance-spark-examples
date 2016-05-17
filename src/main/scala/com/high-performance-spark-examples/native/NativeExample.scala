package com.highperformancespark.examples.ffi

import org.apache.spark.rdd.RDD

import ch.jodersky.jni.nativeLoader

@nativeLoader("libhighPerformanceSpark0")
object NativeExample {
  def jniSum(input: RDD[(String, Array[Int])]): RDD[(String, Int)] = {
    input.mapValues(values => new SumJNI().sum(values))
  }
}
