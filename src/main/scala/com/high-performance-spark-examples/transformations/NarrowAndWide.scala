package com.highperformancespark.examples.transformations

import org.apache.spark.rdd.RDD


object NarrowAndWide {

  //toDO: Probably should write some sort of test for this.
  //this is used in chapter 4 for the stage diagram
  def sillySparkProgram(rdd1 : RDD[Int]) = {

    //tag::narrowWide[]

    //Narrow dependency. Map the rdd to tuples  of (x, 1)
    val rdd2 = rdd1.map(x => (x, 1))
    //wide dependency groupByKey
    val rdd3 = rdd2.groupByKey()
    //end::narrowWide[]

    rdd3
  }
  //this is used in chapter two for the stage diagram.

  //tag::stageDiagram[]
  def simpleSparkProgram(rdd : RDD[Double]): Long ={
  //stage1
    rdd.filter(_< 1000.0)
      .map(x => (x, x) )
  //stage2
      .groupByKey()
      .map{ case(value, groups) => (groups.sum, value)}
  //stage 3
      .sortByKey()
      .count()
  }
  //end::stageDiagram[]

}
