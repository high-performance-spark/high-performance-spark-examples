package com.highperformancespark.examples;

//tag::wordCount[]
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.regex.Pattern;
import java.util.Arrays;

public final class WordCount {
  private static final Pattern pattern = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
    JavaRDD<String> lines = jsc.textFile(args[0]);
    JavaRDD<String> words = lines.flatMap(e -> Arrays.asList(
                                            pattern.split(e)).iterator());
    JavaPairRDD<String, Integer> wordsIntial = words.mapToPair(
      e -> new Tuple2<String, Integer>(e, 1));
  }
}
//end::wordCount[]
