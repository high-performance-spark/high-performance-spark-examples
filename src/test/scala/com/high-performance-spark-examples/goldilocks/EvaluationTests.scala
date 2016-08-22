package com.highperformancespark.examples.goldilocks

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class EvaluationTests extends FunSuite with SharedSparkContext {
  val doubleList = Array(1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0)
  val keyValuePairs =  Array(1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0).zipWithIndex
  val path = "target/testResults"

  test("MapValues preserves Partitioning "){
    val data: RDD[(Double, Int )] = sc.parallelize(keyValuePairs)
    // tag::MapValues[]
    val sortedData = data.sortByKey()
    val mapValues: RDD[(Double, String)] = sortedData.mapValues(_.toString)
    assert(mapValues.partitioner.isDefined, "Using Map Values preserves partitioning")

    val map = sortedData.map( pair => (pair._1, pair._2.toString))
    assert(map.partitioner.isEmpty, "Using map does not preserve partitioning")
    // end::MapValues[]
  }

  test( "Subtract Behavior "){
    // tag::Subtract[]
    val a = Array(1, 2, 3, 4, 4, 4, 4)
    val b = Array(3, 4)
    val rddA = sc.parallelize(a)
    val rddB = sc.parallelize(b)
    val rddC =  rddA.subtract(rddB)
    assert(rddC.count() < rddA.count() - rddB.count())
    // end::Subtract[]
  }

  test( "Intersection Behavior "){
    // tag::Intersect[]
    val a = Array(1, 2, 3, 4, 4, 4, 4)
    val b = Array(3, 4)
    val rddA = sc.parallelize(a)
    val rddB = sc.parallelize(b)
    val intersection =  rddA.intersection(rddB)
    val subtraction = rddA.subtract(rddB)
    val union = intersection.union(subtraction)
    assert(!rddA.collect().sorted.sameElements(union.collect().sorted))
    // end::Intersect[]
  }

  test("Itereative Computations "){
    def RMSE(rdd : RDD[(Int, Int )]) = {
      val n = rdd.count()
      math.sqrt(rdd.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
    }

    val validationSet = sc.parallelize(keyValuePairs)

    // tag::iterativeComp[]
    val testSet: Array[RDD[(Double, Int)]] = Array(validationSet.mapValues(_ + 1), validationSet.mapValues(_ + 2), validationSet)
    validationSet.persist() //persist since we are using this RDD several times
    val errors = testSet.map( rdd => {
        RMSE(rdd.join(validationSet).values)
    })
    // end::iterativeComp[]

    //the one where we didn't change anything should have the lowest root mean squared error
    assert(errors.min == errors(2))

  }

  test( "Two actions without caching ") {
    val rddA: RDD[(Double, Int)] = sc.parallelize(keyValuePairs)

    // tag::TwoActions[]
    val sorted = rddA.sortByKey()
    val count = sorted.count()
    val sample: Long = count / 10
    sorted.take(sample.toInt)
    // end::TwoActions[]
  }

  test( "Two actions with caching  "){
    val rddA: RDD[(Double, Int)] = sc.parallelize(keyValuePairs)
    // tag::TwoActionsCache[]
    val sorted = rddA.sortByKey()
    val count = sorted.count()
    val sample: Long = count / 10
    rddA.persist()
    sorted.take(sample.toInt)
    // end::TwoActionsCache[]
  }



}

