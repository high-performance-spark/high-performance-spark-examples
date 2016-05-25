package com.highperformancespark.examples.goldilocks;

import com.google.common.collect.Sets;
import com.highperformancespark.examples.objects.JavaGoldiLocksRow;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertEquals;

public class JavaQuantileOnlyArtisanalTest extends SharedJavaSparkContext {

  private List<JavaGoldiLocksRow> inputList = Arrays.asList(
    new JavaGoldiLocksRow(0.0, 4.5, 7.7, 5.0),
    new JavaGoldiLocksRow(1.0, 5.5, 6.7, 6.0),
    new JavaGoldiLocksRow(2.0, 5.5, 1.5, 7.0),
    new JavaGoldiLocksRow(3.0, 5.5, 0.5, 7.0),
    new JavaGoldiLocksRow(4.0, 5.5, 0.5, 8.0));

  @Test
  public void goldiLocksFirstTry() {
    SQLContext sqlContext = new SQLContext(jsc());
    DataFrame input = sqlContext.createDataFrame(inputList, JavaGoldiLocksRow.class);
    Map<Integer, Iterable<Double>> secondAndThird = JavaGoldiLocksFirstTry.findRankStatistics(input, Arrays.asList(2L, 3L));

    Map<Integer, Set<Double>> expectedResult = new HashMap<>();
    expectedResult.put(0, new HashSet<>(Arrays.asList(1.0, 2.0)));
    expectedResult.put(1, new HashSet<>(Arrays.asList(5.5, 5.5)));
    expectedResult.put(2, new HashSet<>(Arrays.asList(0.5, 1.5)));
    expectedResult.put(3, new HashSet<>(Arrays.asList(6.0, 7.0)));

    for (Map.Entry<Integer, Iterable<Double>> entry: secondAndThird.entrySet()) {
      Set<Double> resultSet = Sets.newHashSet(entry.getValue());
      Set<Double> expectedSet = expectedResult.get(entry.getKey());

      assertEquals(expectedSet, resultSet);
    }
  }
}
