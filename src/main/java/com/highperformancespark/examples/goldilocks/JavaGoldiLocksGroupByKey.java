package com.highperformancespark.examples.goldilocks;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.*;
import java.util.stream.Collectors;

public class JavaGoldiLocksGroupByKey {
  //tag::groupByKey[]
  public Map<Integer, List<Double>> findRankStatistics(
    JavaPairRDD<Integer, Double> pairRDD, List<Long> ranks) {

    Map<Integer, List<Double>> element_ranks = pairRDD.groupByKey().mapValues(iter -> {
      List<Double> values = new ArrayList<>();
      Iterator<Double> iterator = iter.iterator();
      while (iterator.hasNext())
        values.add(iterator.next());
      Collections.sort(values);

      List<Double> result =
        ranks.stream()
            .map(n -> values.get(new Long(n).intValue()))
            .collect(Collectors.toList());

      return result;
    }).collectAsMap();

    return element_ranks;
  }
  //end::groupByKey[]

}
