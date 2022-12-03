/**
 * Tests that we filter out bad pandas.
 */
package com.highperformancespark.examples.tools

import com.highperformancespark.examples.dataframe.RawPanda
import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite

class FilterInvalidPandasSuite extends FunSuite with SharedSparkContext {
  test("simple filter") {
    val invalidPandas = List(1L, 2L)
    val inputPandas = List(
      RawPanda(1L, "94110", "giant", true, Array(0.0)),
      RawPanda(3L, "94110", "giant", true, Array(0.0)))
    val input = sc.parallelize(inputPandas)
    val result1 =
      FilterInvalidPandas.filterInvalidPandas(sc, invalidPandas, input)
    val result2 =
      FilterInvalidPandas.filterInvalidPandasWithLogs(sc, invalidPandas, input)
    assert(result1.collect() === result2.collect())
    assert(result1.count() === 1)
  }
}
