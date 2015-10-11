/**
 * Happy Panda Example for DataFrames. Computes the % of happy pandas. Very contrived.
 */
import org.apache.spark.sql.DataFrame

object HappyPanda {
  def happyPandas(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("place"),
      (pandaInfo("happy_pandas") / pandaInfo("pandas")).as("percentHappy"))
  }
}
