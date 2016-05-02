package com.highperformancespark.example.SumJNI

class SumJNI {
  @native def sum(n: Array[Int]): Int
}
