package com.highperformancespark.examples.ffi

// tag::sumFJNA[]
import com.sun.jna._
import com.sun.jna.ptr._
object SumFJNA {
  Native.register("high-performance-spark0")
  @native def sumf(n: IntByReference, a: Array[Int]): Int
  def easySum(size: Int, a: Array[Int]): Int = {
    val ns = new IntByReference(size)
    sumf(ns, a)
  }
}
// end::sumFJNA[]
