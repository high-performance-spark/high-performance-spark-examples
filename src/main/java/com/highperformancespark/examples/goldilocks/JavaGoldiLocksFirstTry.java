package com.highperformancespark.examples.goldilocks;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaGoldiLocksFirstTry {

  /**
   * Find nth target rank for every column.
   *
   * For example:
   *
   * dataframe:
   *   (0.0, 4.5, 7.7, 5.0)
   *   (1.0, 5.5, 6.7, 6.0)
   *   (2.0, 5.5, 1.5, 7.0)
   *   (3.0, 5.5, 0.5, 7.0)
   *   (4.0, 5.5, 0.5, 8.0)
   *
   * targetRanks:
   *   1, 3
   *
   * The output will be:
   *   0 -> (0.0, 2.0)
   *   1 -> (4.5, 5.5)
   *   2 -> (7.7, 1.5)
   *   3 -> (5.0, 7.0)
   *
   * @param dataframe dataframe of doubles
   * @param targetRanks the required ranks for every column
   *
   * @return map of (column index, list of target ranks)
   */
  public static Map<Integer, Iterable<Double>> findRankStatistics(DataFrame dataframe, List<Long> targetRanks) {
    JavaPairRDD<Double, Integer> valueColumnPairs = getValueColumnPairs(dataframe);

    JavaPairRDD<Double, Integer> sortedValueColumnPairs = valueColumnPairs.sortByKey();
    sortedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK());

    int numOfColumns = dataframe.schema().length();
    List<Tuple2<Integer, List<Long>>> partitionColumnsFreq =
      getColumnsFreqPerPartition(sortedValueColumnPairs, numOfColumns);

    List<Tuple2<Integer, List<Tuple2<Integer, Long>>>> ranksLocations =
      getRanksLocationsWithinEachPart(targetRanks, partitionColumnsFreq, numOfColumns);

    JavaPairRDD<Integer, Double> targetRanksValues = findTargetRanksIteratively(sortedValueColumnPairs, ranksLocations);

    return targetRanksValues.groupByKey().collectAsMap();
  }

  /**
   * Step 1. Map the rows to pairs of (value, column Index).
   *
   * For example:
   *
   * dataFrame:
   *     1.5, 1.25, 2.0
   *    5.25,  2.5, 1.5
   *
   * The output RDD will be:
   *    (1.5, 0) (1.25, 1) (2.0, 2) (5.25, 0) (2.5, 1) (1.5, 2)
   *
   * @param dataframe dateframe of doubles
   *
   * @return RDD of pairs (value, column Index)
   */
  private static JavaPairRDD<Double, Integer> getValueColumnPairs(DataFrame dataframe) {
    JavaPairRDD<Double, Integer> value_ColIndex =
      dataframe.javaRDD().flatMapToPair((PairFlatMapFunction<Row, Double, Integer>) row -> {
        List<Double> rowList = (List<Double>) (Object) toList(row.toSeq());
        List<Tuple2<Double, Integer>> list = zipWithIndex(rowList);
        return list;
      });

    return value_ColIndex;
  }

  /**
   * Step 2. Find the number of elements for each column in each partition.
   *
   * For Example:
   *
   * sortedValueColumnPairs:
   *    Partition 1: (1.5, 0) (1.25, 1) (2.0, 2) (5.25, 0)
   *    Partition 2: (7.5, 1) (9.5, 2)
   *
   * numOfColumns: 3
   *
   * The output will be:
   *    [(0, [2, 1, 1]), (1, [0, 1, 1])]
   *
   * @param sortedValueColumnPairs - sorted RDD of (value, column Index) pairs
   * @param numOfColumns the number of columns
   *
   * @return Array that contains (partition index, number of elements from every column on this partition)
   */
  private static List<Tuple2<Integer, List<Long>>> getColumnsFreqPerPartition(JavaPairRDD<Double, Integer> sortedValueColumnPairs, int numOfColumns) {
    List<Tuple2<Integer, List<Long>>> columsFreqPerPartition =
      sortedValueColumnPairs.mapPartitionsWithIndex((partitionIndex, valueColumnPairs) -> {
        Long[] freq = new Long[numOfColumns];
        Arrays.fill(freq, 0L);

        while(valueColumnPairs.hasNext()) {
          int colIndex = valueColumnPairs.next()._2;
          freq[colIndex] = freq[colIndex] + 1;
        }

        List<Long> freqList = Arrays.asList(freq);
        List<Tuple2<Integer, List<Long>>> partitionList = Arrays.asList(new Tuple2<>(partitionIndex, freqList));
        return partitionList.iterator();
      }, false).collect();

    return columsFreqPerPartition;
  }

  /**
   * Step 3: For each Partition determine the index of the elements that are desired rank statistics
   *
   * For Example:
   *    targetRanks: 5
   *    partitionColumnsFreq: [(0, [2, 3]), (1, [4, 1]), (2, [5, 2])]
   *    numOfColumns: 2
   *
   * The output will be:
   *    [(0, []), (1, [(colIdx=0, rankLocation=3)]), (2, [(colIndex=1, rankLocation=1)])]
   *
   * @param partitionColumnsFreq Array of (partition index, columns frequencies per this partition)
   *
   * @return  Array that contains (partition index, relevantIndexList where relevantIndexList(i) = the index
   *          of an element on this partition that matches one of the target ranks)
   */
  private static List<Tuple2<Integer, List<Tuple2<Integer, Long>>>> getRanksLocationsWithinEachPart(List<Long> targetRanks,
      List<Tuple2<Integer, List<Long>>> partitionColumnsFreq, int numOfColumns) {

    long[] runningTotal = new long[numOfColumns];

    List<Tuple2<Integer, List<Tuple2<Integer, Long>>>> ranksLocations =
      partitionColumnsFreq
        .stream()
        .sorted((o1, o2) -> o1._1.compareTo(o2._1))
        .map(partitionIndex_columnsFreq -> {
          int partitionIndex = partitionIndex_columnsFreq._1;
          List<Long> columnsFreq = partitionIndex_columnsFreq._2;

          List<Tuple2<Integer, Long>> relevantIndexList = new ArrayList<>();

          zipWithIndex(columnsFreq).stream().forEach(colCount_colIndex -> {
            long colCount = colCount_colIndex._1;
            int colIndex = colCount_colIndex._2;

            long runningTotalCol = runningTotal[colIndex];
            Stream<Long> ranksHere =
              targetRanks.stream().filter(rank -> runningTotalCol < rank && runningTotalCol + colCount >= rank);

            // for each of the rank statistics present add this column index and the index it will be at
            // on this partition (the rank - the running total)
            relevantIndexList.addAll(
              ranksHere.map(rank -> new Tuple2<>(colIndex, rank - runningTotalCol)).collect(Collectors.toList()));

            runningTotal[colIndex] += colCount;
          });


          return new Tuple2<>(partitionIndex, relevantIndexList);
        }).collect(Collectors.toList());

    return ranksLocations;
  }

  /**
   * Finds rank statistics elements using ranksLocations.
   *
   * @param sortedValueColumnPairs - sorted RDD of (value, colIndex) pairs
   * @param ranksLocations Array of (partition Index, list of (column index, rank index of this column at this partition))
   *
   * @return returns RDD of the target ranks (column index, value)
   */
  private static JavaPairRDD<Integer, Double> findTargetRanksIteratively(JavaPairRDD<Double, Integer> sortedValueColumnPairs,
      List<Tuple2<Integer, List<Tuple2<Integer, Long>>>> ranksLocations) {

    JavaRDD<Tuple2<Integer, Double>> targetRanks = sortedValueColumnPairs.mapPartitionsWithIndex(
      (partitionIndex, valueColumnPairs) -> {
        List<Tuple2<Integer, Long>> targetsInThisPart = ranksLocations.get(partitionIndex)._2;
        List<Tuple2<Integer, Double>> result = new ArrayList<>();

        if (!targetsInThisPart.isEmpty()) {
          Map<Integer, List<Long>> columnsRelativeIndex = groupByKey(targetsInThisPart);
          Set<Integer> columnsInThisPart = columnsRelativeIndex.keySet();

          Map<Integer, Long> runningTotals = toMap(columnsInThisPart);

          // filter this iterator, so that it contains only those (value, columnIndex) that are the ranks statistics on this partition
          // I.e. Keep track of the number of elements we have seen for each columnIndex using the
          // running total hashMap. Keep those pairs for which value is the nth element for that columnIndex that appears on this partition
          // and the map contains (columnIndex, n).

          while (valueColumnPairs.hasNext()) {
            Tuple2<Double, Integer> value_colIndex = valueColumnPairs.next();
            double value = value_colIndex._1;
            int colIndex = value_colIndex._2;

            if (columnsInThisPart.contains(colIndex)) {
              long total = runningTotals.get(colIndex) + 1L;
              runningTotals.put(colIndex, total);
              if (columnsRelativeIndex.get(colIndex).contains(total)) {
                result.add(value_colIndex.swap());
              }
            }
          }
        }

        return result.iterator();
      }, false);

    return targetRanks.mapToPair((PairFunction<Tuple2<Integer, Double>, Integer, Double>) t -> t);
  }

  private static Map<Integer,Long> toMap(Set<Integer> set) {
    Map<Integer, Long> map = new HashMap<>();
    for (int k: set)
      map.put(k, 0L);

    return map;
  }

  private static Map<Integer, List<Long>> groupByKey(List<Tuple2<Integer, Long>> list) {
    Map<Integer, List<Long>> map = new HashMap<>();
    for (int i = 0; i < list.size(); i++) {
      Tuple2<Integer, Long> curr = list.get(i);
      if (!map.containsKey(curr._1))
        map.put(curr._1, new ArrayList<>());

      map.get(curr._1).add(curr._2);
    }

    return map;
  }

  private static<T> List<T> toList(scala.collection.Seq<T> seq) {
    return scala.collection.JavaConversions.seqAsJavaList(seq);
  }

  private static<T> List<Tuple2<T, Integer>> zipWithIndex(List<T> list) {
    List<Tuple2<T, Integer>> indexedList = new ArrayList<>();
    for (int i = 0; i < list.size(); i++)
      indexedList.add(new Tuple2<>(list.get(i), i));

    return indexedList;
  }

}

