package com.highperformancespark.examples.ffi

object StandAlone {
  def main(args: Array[String]) {
    System.loadLibrary("highPerformanceSpark0")
    println(new SumJNI().sum(Array(1,2,3)))
  }
}
