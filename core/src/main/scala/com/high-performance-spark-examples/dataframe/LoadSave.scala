/**
 * Load and save data to/from DataFrames
 */
package com.highperformancespark.examples.dataframe

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class LoadSave(sc: SparkContext, session: SparkSession) {
  import session.implicits._
  //tag::createFromRDD[]
  def createFromCaseClassRDD(input: RDD[PandaPlace]) = {
    // Create DataFrame explicitly using session and schema inference
    val df1 = session.createDataFrame(input)

    // Create DataFrame using session implicits and schema inference
    val df2 = input.toDF()

    // Create a Row RDD from our RDD of case classes
    val rowRDD = input.map(pm => Row(pm.name,
      pm.pandas.map(pi => Row(pi.id, pi.zip, pi.happy, pi.attributes))))

    val pandasType = ArrayType(StructType(List(
      StructField("id", LongType, true),
      StructField("zip", StringType, true),
      StructField("happy", BooleanType, true),
      StructField("attributes", ArrayType(FloatType), true))))

    // Create DataFrame explicitly with specified schema
    val schema = StructType(List(StructField("name", StringType, true),
      StructField("pandas", pandasType)))

    val df3 = session.createDataFrame(rowRDD, schema)
  }
  //end::createFromRDD[]

  //tag::createFromRDDBasic[]
  def createFromCaseClassRDD(input: Seq[PandaPlace]) = {
    val rdd = sc.parallelize(input)
    // Create DataFrame explicitly using session and schema inference
    val df1 = session.createDataFrame(input)
  }
  //end::createFromRDDBasic[]

  //tag::createGetSchema[]
  def createAndPrintSchema() = {
    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 0.1))
    val pandaPlace = PandaPlace("toronto", Array(damao))
    val df = session.createDataFrame(Seq(pandaPlace))
    df.printSchema()
  }
  //end::createGetSchema[]

  //tag::createFromLocal[]
  def createFromLocal(input: Seq[PandaPlace]) = {
    session.createDataFrame(input)
  }
  //end::createFromLocal[]

  //tag::collectResults[]
  def collectDF(df: DataFrame) = {
    val result: Array[Row] = df.collect()
    result
  }
  //end::collectResults[]

  //tag::toRDD[]
  def toRDD(input: DataFrame): RDD[RawPanda] = {
    val rdd: RDD[Row] = input.rdd
    rdd.map(row => RawPanda(row.getAs[Long](0), row.getAs[String](1),
      row.getAs[String](2), row.getAs[Boolean](3), row.getAs[Array[Double]](4)))
  }
  //end::toRDD[]

  //tag::partitionedOutput[]
  def writeOutByZip(input: DataFrame): Unit = {
    input.write.partitionBy("zipcode").format("json").save("output/")
  }
  //end::partitionedOutput[]

  //tag::saveAppend[]
  def writeAppend(input: DataFrame): Unit = {
    input.write.mode(SaveMode.Append).save("output/")
  }
  //end::saveAppend[]

  def createJDBC() = {
    //tag::createJDBC[]
    session.read.jdbc("jdbc:dialect:serverName;user=user;password=pass",
      "table", new Properties)

    session.read.format("jdbc")
      .option("url", "jdbc:dialect:serverName")
      .option("dbtable", "table").load()
    //end::createJDBC[]
  }

  def writeJDBC(df: DataFrame) = {
    //tag::writeJDBC[]
    df.write.jdbc("jdbc:dialect:serverName;user=user;password=pass",
      "table", new Properties)

    df.write.format("jdbc")
      .option("url", "jdbc:dialect:serverName")
      .option("user", "user")
      .option("password", "pass")
      .option("dbtable", "table").save()
    //end::writeJDBC[]
  }

  //tag::loadParquet[]
  def loadParquet(path: String): DataFrame = {
    // Configure Spark to read binary data as string,
    // note: must be configured on session.
    session.conf.set("spark.sql.parquet.binaryAsString", "true")

    // Load parquet data using merge schema (configured through option)
    session.read
      .option("mergeSchema", "true")
      .format("parquet")
      .load(path)
  }
  //end::loadParquet[]

  //tag::writeParquet[]
  def writeParquet(df: DataFrame, path: String) = {
    df.write.format("parquet").save(path)
  }
  //end::writeParquet[]

  //tag::loadHiveTable[]
  def loadHiveTable(): DataFrame = {
    session.read.table("pandas")
  }
  //end::loadHiveTable[]

  //tag::saveManagedTable[]
  def saveManagedTable(df: DataFrame): Unit = {
    df.write.saveAsTable("pandas")
  }
  //end::saveManagedTable[]
}
