package com.highperformancespark.examples.errors

import com.holdenkarau.spark.testing._

import org.scalatest.FunSuite

class ThrowsSuite extends FunSuite with SharedSparkContext {
  test("inner throw & outer throw should both throw SparkExceptions exceptions") {
    intercept[org.apache.spark.SparkException] {
      Throws.throwInner(sc)
    }
    intercept[org.apache.spark.SparkException] {
      Throws.throwOuter(sc)
    }
    intercept[org.apache.spark.SparkException] {
      Throws.throwInner2(sc)
    }
    intercept[org.apache.spark.SparkException] {
      Throws.throwOuter2(sc)
    }
  }

  test("loading missing data should throw") {
    intercept[org.apache.hadoop.mapred.InvalidInputException] {
      Throws.nonExistantInput(sc)
    }
  }
}
