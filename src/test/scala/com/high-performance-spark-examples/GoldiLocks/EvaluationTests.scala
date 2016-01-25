package com.highperformancespark.examples.goldilocks

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class EvaluationTests extends FunSuite with SharedSparkContext {

  test("MapValues preserves Partitioning "){
    val s = Array(1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0).zipWithIndex
    val data: RDD[(Double, Int )] = sc.parallelize(s)

    // tag::MapValues[]
    val sortedData = data.sortByKey()
    val mapValues: RDD[(Double, String)] = sortedData.mapValues(_.toString)
    assert(mapValues.partitioner.isDefined, "Using Map Values preserves partitioning")

    val map = sortedData.map( pair => (pair._1, pair._2.toString))
    assert(!map.partitioner.isDefined, "Using map does not preserve partitioning")
    // end::MapValues[]
  }

  test( "Subtract Behavior "){
    val a = Array(1, 2, 3 ,4 ,4 ,4 ,4 )
    val b = Array(3, 4 )
    val rddA = sc.parallelize(a)
    val rddB = sc.parallelize(b)
    val rddC =  rddA.subtract(rddB)
    assert(rddC.count() < rddA.count() - rddB.count())
  }

}

