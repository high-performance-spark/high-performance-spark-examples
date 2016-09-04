package com.highperformancespark.examples.ml

import org.apache.spark.ml.classification._

object SimpleExport {
  //tag::exportLR[]
  def exportLRToCSV(model: LogisticRegressionModel) = {
    (model.coefficients.toArray :+ model.intercept).mkString(",")
  }
  //end::exportLR[]
}
