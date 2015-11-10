/**
 * What sort of big data book would this be if we didn't mention wordcount?
 */
import org.apache.spark.rdd._

object WordCount {
  // bad idea: uses group by key
  def badIdea(rdd: RDD[String]): RDD[(String, Int)] = {
    val words = rdd.flatMap(_.split(" "))
    val wordPairs = words.map((_, 1))
    val grouped = wordPairs.groupByKey()
    val wordCounts = grouped.mapValues(_.sum)
    wordCounts
  }

  // good idea: doesn't use group by key
  def goodIdea(rdd: RDD[String]): RDD[(String, Int)] = {
    val words = rdd.flatMap(_.split(" "))
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)
    wordCounts
  }
}
