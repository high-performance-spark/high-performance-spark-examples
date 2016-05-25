package com.highperformancespark.examples


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class JavaInteropTestHelper(sc: SparkContext) {
  def generateMiniPairRDD(): RDD[(String, Long)] = {
    sc.parallelize(List(("panda", 12L)))
  }
}
