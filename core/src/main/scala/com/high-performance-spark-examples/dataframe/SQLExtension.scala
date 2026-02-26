/** Extension for the SparkSession to allow us to plug in a custom optimizer
  */

package com.highperformancespark.examples.dataframe

import org.apache.spark.sql.{
  SparkSessionExtensions,
  SparkSessionExtensionsProvider
}

class SQLExtension extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // There are _many different_ types of rules you can inject, here we're focused on
    // making things go fast so our sample is an optimizer rule (AQE rules could also make sense).
    extensions.injectOptimizerRule(session => NullabilityFilterOptimizer)
  }
}
