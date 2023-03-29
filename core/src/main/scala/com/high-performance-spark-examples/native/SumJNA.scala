package com.highperformancespark.examples.ffi

// tag::sumJNA[]
import com.sun.jna._
object SumJNA {
  Native.register("high-performance-spark0")
  @native def sum(n: Array[Int], size: Int): Int
}
// end::sumJNA[]
