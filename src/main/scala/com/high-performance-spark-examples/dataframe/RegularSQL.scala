/**
 * Using plain-old-sql
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.thriftserver._

case class RegularSQL(sqlContext: SQLContext) {
  import sqlContext.implicits._

  //tag::queryTable[]
  def querySQL(): DataFrame = {
    sqlContext.sql("SELECT * FROM pandas WHERE size > 0")
  }
  //end::queryTable[]

  // TODO: Holden: include a parquet example file and point this to that.
  //tag::queryRawFile[]
  def queryRawFile(): DataFrame = {
    sqlContext.sql("SELECT * FROM parquet.`path_to_parquer_file`")
  }
  //end::queryRawFile[]

  //tag::registerTable[]
  def registerTable(df: DataFrame): Unit = {
    df.registerTempTable("pandas")
    df.saveAsTable("perm_pandas")
  }
  //end::registerTable[]
}
