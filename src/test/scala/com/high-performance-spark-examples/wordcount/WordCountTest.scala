package com.highperformancespark.examples.wordcount


import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class WordCountTest extends FunSuite with SharedSparkContext {
  test("word count with Stop Words Removed"){
    val wordRDD = sc.parallelize(Seq(
      "How happy was the panda? You ask.",
      "Panda is the most happy panda in all the #$!?ing land!"))

    val stopWords: Set[String] = Set("a", "the", "in", "was", "there", "she", "he")
    val illegalTokens: Array[Char] = "#$%?!.".toCharArray

    val wordCounts = WordCount.withStopWordsFiltered(
      wordRDD, illegalTokens, stopWords)
    val wordCountsAsMap = wordCounts.collectAsMap()
    assert(!wordCountsAsMap.contains("the"))
    assert(!wordCountsAsMap.contains("?"))
    assert(!wordCountsAsMap.contains("#$!?ing"))
    assert(wordCountsAsMap.contains("ing"))
    assert(wordCountsAsMap.get("panda").get.equals(3))
  }

  test("word count with simple counting") {
    val wordRDD = sc.parallelize(
      Seq(
        "a b c d", 
        "b c d e"
      )
    )
    val wordCounts = WordCount.simpleWordCount(wordRDD)

    val wordCountsAsMap = wordCounts.collectAsMap()

    for (character <- 'a' to 'e') {
      assert(wordCountsAsMap.contains(character.toString))
    }
    
    for (character <- 'b' to 'd') {
      assert(wordCountsAsMap.get(character.toString).get == 2)
    }
  }

  test("word count with bad idea") {
    val wordRDD = sc.parallelize(
      Seq(
        "a b c d", 
        "b c d e"
      )
    )
    val wordCounts = WordCount.badIdea(wordRDD)

    val wordCountsAsMap = wordCounts.collectAsMap()

    for (character <- 'a' to 'e') {
      assert(wordCountsAsMap.contains(character.toString))
    }
    
    for (character <- 'b' to 'd') {
      assert(wordCountsAsMap.get(character.toString).get == 2)
    }
  }
}
