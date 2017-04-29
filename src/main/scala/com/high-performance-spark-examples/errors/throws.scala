package com.highperformancespark.examples.errors

import org.apache.spark._
import org.apache.spark.rdd.RDD

object Throws {
  def throwInner(sc: SparkContext) = {
    //tag::throwInner1[]
    val data = sc.parallelize(List(1, 2, 3))
    // Will throw an exception when forced to evaluate
    val transform1 = data.map(x => x/0)
    val transform2 = transform1.map(x => x + 1)
    transform2.collect() // Forces evaluation
    //end::throwInner1[]
  }

  def throwOuter(sc: SparkContext) = {
    //tag::throwOuter1[]
    val data = sc.parallelize(List(1, 2, 3))
    val transform1 = data.map(x => x + 1)
    // Will throw an exception when forced to evaluate
    val transform2 = transform1.map(x => x/0)
    transform2.collect() // Forces evaluation
    //end::throwOuter1[]
  }

  //tag::badFunctions[]
  def add1(x: Int): Int = {
    x + 1
  }

  def divZero(x: Int): Int = {
    x / 0
  }
  //end::badFunctions[]

  //tag::badEx3[]
  def throwInner2(sc: SparkContext) = {
    val data = sc.parallelize(List(1, 2, 3))
    // Will throw an exception when forced to evaluate
    val transform1 = data.map(divZero)
    val transform2 = transform1.map(add1)
    transform2.collect() // Forces evaluation
  }

  def throwOuter2(sc: SparkContext) = {
    val data = sc.parallelize(List(1, 2, 3))
    val transform1 = data.map(add1)
    // Will throw an exception when forced to evaluate
    val transform2 = transform1.map(divZero)
    transform2.collect() // Forces evaluation
  }
  //end::badEx3

  def nonExistentInput(sc: SparkContext) = {
    //tag::nonExistentInput[]
    val input = sc.textFile("file:///doesnotexist.txt")
    val data = input.map(x => x.toInt)
    val transform = data.map(x => x + 1)
    transform.collect() // Forces evaluation
    //end::nonExistentInput[]
  }
}
