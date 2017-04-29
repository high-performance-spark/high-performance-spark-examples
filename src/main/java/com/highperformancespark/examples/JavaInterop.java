package com.highperformancespark.examples;

import scala.reflect.*;
import scala.Tuple2;

import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class JavaInterop {

  //tag::realClassTag[]
  public static JavaPairRDD wrapPairRDD(
    RDD<Tuple2<String, Object>> rdd) {
    // Construct the class tags
    ClassTag<String> strCt = ClassTag$.MODULE$.apply(String.class);
    ClassTag<Long> longCt = ClassTag$.MODULE$.apply(scala.Long.class);
    return new JavaPairRDD(rdd, strCt, longCt);
  }
  //end::realClassTag[]

  //tag::fakeClassTag[]
  public static JavaPairRDD wrapPairRDDFakeCt(
    RDD<Tuple2<String, Object>> rdd) {
    // Construct the class tags by casting AnyRef - this would be more commonly done
    // with generic or templated code where we can't explicitly construct the correct
    // class tag as using fake class tags may result in degraded performance.
    ClassTag<Object> fake = ClassTag$.MODULE$.AnyRef();
    return new JavaPairRDD(rdd, fake, fake);
  }
  //end::fakeClassTag[]
}
