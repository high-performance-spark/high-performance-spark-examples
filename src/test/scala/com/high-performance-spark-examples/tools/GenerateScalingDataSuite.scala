/**
 * Verify that generate scaling data returns results
 */
package com.highperformancespark.examples.tools

import com.highperformancespark.examples.dataframe.RawPanda

import com.holdenkarau.spark.testing._

import org.scalatest.FunSuite

class GeneratescalaingDataSuite extends FunSuite with SharedSparkContext {
  // The number of entries depends somewhat on the partition split because we
  // zip multiple separate RDDs so its more of a "request"
  test("expected num entries") {
    val result = GenerateScalingData.generateFullGoldilocks(sc, 10L, 20)
    assert(result.count() <= 10)
    assert(result.count() > 5)
    assert(result.map(_.id).distinct().count() > 1)
  }

  test("expected num entries same id") {
    val result = GenerateScalingData.generateGoldilocks(sc, 5L, 20)
    assert(result.count() <= 5)
    assert(result.count() >= 2)
    assert(result.map(_.id).distinct().count() == 1)
  }

  test("mini scale data") {
    val result = GenerateScalingData.generateMiniScale(sc, 20L, 1)
    assert(result.count() <= 20)
    assert(result.count() > 5)
    assert(result.map(_._1).distinct().count() > 1)
  }

  test("mini scale rows") {
    val result = GenerateScalingData.generateMiniScaleRows(sc, 20L, 1)
    assert(result.count() <= 20)
    assert(result.count() > 5)
    assert(result.map(_(0)).distinct().count() > 1)
  }
}
