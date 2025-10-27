package com.highperformancespark.examples.gpu

import org.apache.spark.sql.SparkSession
import org.apache.spark.resource._
import org.apache.spark.resource.ResourceProfileBuilder
import org.apache.spark.TaskContext

object GPUResourceProfileExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GPUResourceProfileExample")
      .getOrCreate()
    run(spark)
  }

  def run(spark: SparkSession) = {
    val sc = spark.sparkContext
    //tag::gpuResourceProfileExample[]
    // Create a resource profile requesting 2 NVIDIA GPUs per executor and 1 per task
    val gpuResourceProfile = new ResourceProfileBuilder()
      .require(new ExecutorResourceRequests().resource("gpu", 2))
      .require(new TaskResourceRequests().resource("gpu", 1))
      .build()

    // Use resource profile to run on a machine with GPUs.
    val rdd = sc.parallelize(1 to 4, 4)
      .withResources(gpuResourceProfile)
      .map { i =>
        // Do some special GPU stuff here my friend
        i
      }
    //end::gpuResourceProfileExample[]

    rdd.collect().foreach(println)

    spark.stop()
  }
}
