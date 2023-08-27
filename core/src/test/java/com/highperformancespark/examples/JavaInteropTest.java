package com.highperformancespark.examples;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import scala.Tuple2;

import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaInteropTest extends SharedJavaSparkContext {
  
  @Test
  public void wrapPairRDDTest() {
    JavaInteropTestHelper helper = new JavaInteropTestHelper(sc());
    JavaInterop ji = new JavaInterop();
    RDD<Tuple2<String, Object>> rdd = helper.generateMiniPairRDD();
    JavaPairRDD prdd = ji.wrapPairRDD(rdd);
    List<Tuple2<String, Long>> expected = Arrays.asList(new Tuple2<String, Long>("panda", 12L));
    assertEquals(expected, prdd.collect());
  }

  @Test
  public void wrapPairRDDFakeCtTest() {
    JavaInteropTestHelper helper = new JavaInteropTestHelper(sc());
    JavaInterop ji = new JavaInterop();
    RDD<Tuple2<String, Object>> rdd = helper.generateMiniPairRDD();
    JavaPairRDD prdd = ji.wrapPairRDDFakeCt(rdd);
    List<Tuple2<String, Long>> expected = Arrays.asList(new Tuple2<String, Long>("panda", 12L));
    assertEquals(expected, prdd.collect());
  }
}
