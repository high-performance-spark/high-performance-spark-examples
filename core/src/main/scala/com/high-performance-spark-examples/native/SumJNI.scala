package com.highperformancespark.examples.ffi

import com.github.sbt.jni.nativeLoader

//tag::sumJNIDecorator[]
@nativeLoader("high-performance-spark0")
//end::sumJNIDecorator[]
// tag::sumJNI[]
class SumJNI {
  @native def sum(n: Array[Int]): Int
}
// end::sumJNI[]
