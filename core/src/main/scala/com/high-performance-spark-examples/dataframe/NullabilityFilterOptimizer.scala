/**
 * Extension for the SparkSession to allow us to plug in a custom optimizer
 */

package com.highperformancespark.examples.dataframe

import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.expressions.{And, IsNotNull}

object NullabilityFilterOptimizer extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case p @ Project(projectList, projChild) =>
        val children = projectList.flatMap(_.children)
        // If there are no null intolerant children don't worry about it
        if (children.isEmpty) {
          p
        } else {
          val filterCond = children.map(IsNotNull(_)).reduceLeft(And)
          Project(projectList, Filter(filterCond, projChild))
        }
    }
  }
}
