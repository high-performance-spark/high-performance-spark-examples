package com.highperformancespark.examples.ffi

object StandAlone {
  // $COVERAGE-OFF$
  def main(args: Array[String]) {
    //tag::systemLoadLibrary[]
    System.loadLibrary("highPerformanceSpark0")
    //end::systemLoadLibrary[]
    println(new SumJNI().sum(Array(1,2,3)))
  }
   // $COVERAGE-ON$
}
