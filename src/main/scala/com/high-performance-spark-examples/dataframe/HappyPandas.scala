/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
import org.apache.spark.sql.DataFrame

object HappyPanda {
  def happyPandas(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("place"),
      (pandaInfo("happyPandas") / pandaInfo("totalPandas")).as("percentHappy"))
  }
  def minHappyPandas(pandaInfo: DataFrame, num: Int): DataFrame = {
    pandaInfo.filter(pandaInfo("happyPandas") >= num)
  }
}
