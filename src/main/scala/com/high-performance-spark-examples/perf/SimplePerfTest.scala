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

import com.highperformancespark.examples.dataframe.RawPanda
import com.highperformancespark.examples.tools._

import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.types._

/**
 * A simple performance test to compare a simple sort between DataFrame, and RDD
 */
object SimplePerfTest {
  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("simple-perf-test")
    val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = sparkSession.sparkContext
    val scalingFactor = if (args.length > 0) args(0).toLong else 100L
    val size = if (args.length > 1) args(1).toInt else 50
    run(sc, sparkSession, scalingFactor, size)
  }

  def run(sc: SparkContext, session: SparkSession,
    scalingFactor: Long, size: Int) = {
    import session.implicits._
    val inputRDD = GenerateScalingData.generateFullGoldilocks(
      sc, scalingFactor, size)
    val pairRDD = inputRDD.map(p => (p.zip.toInt, p.attributes(0)))
    pairRDD.cache()
    pairRDD.count()
    val rddTimeings = 1.to(10).map(x => time(testOnRDD(pairRDD)))
    val groupTimeings = 1.to(10).map(x => time(groupOnRDD(pairRDD)))
    val df = inputRDD.toDF()
    val inputDataFrame = df.select(
      df("zip").cast(IntegerType),
      df("attributes")(0).as("fuzzyness").cast(DoubleType))
    inputDataFrame.cache()
    inputDataFrame.count()
    val dataFrameTimeings = 1.to(10).map(x => time(testOnDataFrame(inputDataFrame)))
    println(rddTimeings.map(_._2).mkString(","))
    println(groupTimeings.map(_._2).mkString(","))
    println(dataFrameTimeings.map(_._2).mkString(","))
  }

  def testOnRDD(rdd: RDD[(Int, Double)]) = {
    rdd.map{case (x, y) => (x, (y, 1))}
      .reduceByKey{case (x, y) => (x._1 + y._1, x._2 + y._2)}.count()
  }

  def groupOnRDD(rdd: RDD[(Int, Double)]) = {
    rdd.groupByKey().mapValues{v =>
      v.aggregate((0.0, 0))({case (x, y) => (x._1 + y, x._2 + 1)},
        {case (x, y) => (x._1 + y._1, x._2 + y._2)})}.count()
  }

  def testOnDataFrame(df: DataFrame) = {
    df.groupBy("zip").avg("fuzzyness").count()
  }

  def time[R](block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Time ${t1 - t0}ns")
    (result, t1 - t0)
  }
}
