/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.highperformancespark.examples.perf

import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * A simple performance test to compare a simple sort between DataFrame, Dataset, and RDD
 */
object SimplePerfTest {
  def main(args: Array[Sttring]) = {
    val sparkConf = SparkConf().setAppName("simple-perf-test")
    val sc = SparkContext(sparkConf)
    val scalingFactor = if (args.length > 0) args(0).toInt else 100
    val size = if (args.length > 1) args(1).toInt else 50
    val inputRDD = GenerateScalingData.generateGoldilocks(sc, scalingFactor, size)
  }
}
