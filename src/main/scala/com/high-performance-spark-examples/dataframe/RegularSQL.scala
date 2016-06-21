/**
 * Using plain-old-sql
 */
package com.highperformancespark.examples.dataframe

import org.apache.spark.sql._

case class RegularSQL(sqlContext: SQLContext) {

  //tag::queryTable[]
  def querySQL(): DataFrame = {
    sqlContext.sql("SELECT * FROM pandas WHERE size > 0")
  }
  //end::queryTable[]

  // TODO: Holden: include a parquet example file and point this to that.
  //tag::queryRawFile[]
  def queryRawFile(): DataFrame = {
    sqlContext.sql("SELECT * FROM parquet.`path_to_parquet_file`")
  }
  //end::queryRawFile[]

  //tag::registerTable[]
  def registerTable(df: DataFrame): Unit = {
    df.registerTempTable("pandas")
    df.write.saveAsTable("perm_pandas")
  }
  //end::registerTable[]
}
