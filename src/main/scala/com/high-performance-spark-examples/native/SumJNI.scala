package com.highperformancespark.examples.ffi

class SumJNI {
  @native def sum(n: Array[Int]): Int
}
