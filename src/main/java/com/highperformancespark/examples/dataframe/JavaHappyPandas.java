package com.highperformancespark.examples.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.hive.HiveContext;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class JavaHappyPandas {

  /**
   * Creates SQLContext with an existing SparkContext.
   */
  public static SQLContext sqlContext(JavaSparkContext jsc) {
    SQLContext sqlContext = new SQLContext(jsc);
    return sqlContext;
  }

  /**
   * Creates HiveContext with an existing SparkContext.
   */
  public static HiveContext hiveContext(JavaSparkContext jsc) {
    HiveContext hiveContext = new HiveContext(jsc);
    return hiveContext;
  }

  /**
   * Illustrate loading some JSON data.
   */
  public static DataFrame loadDataSimple(JavaSparkContext jsc, SQLContext sqlContext, String path) {
    DataFrame df1 = sqlContext.read().json(path);

    DataFrame df2 = sqlContext.read().format("json").option("samplingRatio", "1.0").load(path);

    JavaRDD<String> jsonRDD = jsc.textFile(path);
    DataFrame df3 = sqlContext.read().json(jsonRDD);

    return df1;
  }

  public static DataFrame jsonLoadFromRDD(SQLContext sqlContext, JavaRDD<String> input) {
    JavaRDD<String> rdd = input.filter(e -> e.contains("panda"));
    DataFrame df = sqlContext.read().json(rdd);
    return df;
  }

  //  Here will be some examples on PandaInfo DataFrame

  /**
   * Gets the percentage of happy pandas per place.
   *
   * @param pandaInfo the input DataFrame
   * @return Returns DataFrame of (place, percentage of happy pandas)
   */
  public static DataFrame happyPandasPercentage(DataFrame pandaInfo) {
    DataFrame happyPercentage = pandaInfo.select(pandaInfo.col("place"),
      pandaInfo.col("happyPandas").divide(pandaInfo.col("totalPandas")).as("percentHappy"));
    return happyPercentage;
  }

  /**
   * Encodes pandaType to Integer values instead of String values.
   *
   * @param pandaInfo the input DataFrame
   * @return Returns a DataFrame of pandaId and integer value for pandaType.
   */
  public static DataFrame encodePandaType(DataFrame pandaInfo) {
    DataFrame encodedDF = pandaInfo.select(pandaInfo.col("id"),
        when(pandaInfo.col("pt").equalTo("giant"), 0).
        when(pandaInfo.col("pt").equalTo("red"), 1).
        otherwise(2).as("encodedType"));

    return encodedDF;
  }

  /**
   * Gets places with happy pandas more than minHappinessBound.
   */
  public static DataFrame minHappyPandas(DataFrame pandaInfo, int minHappyPandas) {
    return pandaInfo.filter(pandaInfo.col("happyPandas").geq(minHappyPandas));
  }

  /**
   * Find pandas that are sad.
   */
  public static DataFrame sadPandas(DataFrame pandaInfo) {
    return pandaInfo.filter(pandaInfo.col("happy").notEqual(true));
  }

  /**
   * Find pandas that are happy and fuzzier than squishy.
   */
  public static DataFrame happyFuzzyPandas(DataFrame pandaInfo) {
    DataFrame df = pandaInfo.filter(
      pandaInfo.col("happy").and(pandaInfo.col("attributes").apply(0)).gt(pandaInfo.col("attributes").apply(1))
    );

    return df;
  }

  /**
   * Gets places that contains happy pandas more than unhappy pandas.
   */
  public static DataFrame happyPandasPlaces(DataFrame pandaInfo) {
    return pandaInfo.filter(pandaInfo.col("happyPandas").geq(pandaInfo.col("totalPandas").divide(2)));
  }

  /**
   * Remove duplicate pandas by id.
   */
  public static DataFrame removeDuplicates(DataFrame pandas) {
    DataFrame df = pandas.dropDuplicates(new String[]{"id"});
    return df;
  }

  public static DataFrame describePandas(DataFrame pandas) {
    return pandas.describe();
  }

  public static DataFrame maxPandaSizePerZip(DataFrame pandas) {
    return pandas.groupBy(pandas.col("zip")).max("pandaSize");
  }

  public static DataFrame minMaxPandaSizePerZip(DataFrame pandas) {
    return pandas.groupBy(pandas.col("zip")).agg(min("pandaSize"), max("pandaSize"));
  }

  public static DataFrame minPandaSizeMaxAgePerZip(DataFrame pandas) {
    Map<String, String> map = new HashMap<>();
    map.put("pandaSize", "min");
    map.put("age", "max");

    DataFrame df = pandas.groupBy(pandas.col("zip")).agg(map);
    return df;
  }

  public static DataFrame minMeanSizePerZip(DataFrame pandas) {
    return pandas.groupBy(pandas.col("zip")).agg(min(pandas.col("pandaSize")), mean(pandas.col("pandaSize")));
  }

  public static DataFrame simpleSqlExample(DataFrame pandas) {
    SQLContext sqlContext = pandas.sqlContext();
    pandas.registerTempTable("pandas");

    DataFrame miniPandas = sqlContext.sql("SELECT * FROM pandas WHERE pandaSize < 12");
    return miniPandas;
  }

  /**
   * Orders pandas by size ascending and by age descending.
   * Pandas will be sorted by "size" first and if two pandas
   * have the same "size"  will be sorted by "age".
   */
  public static DataFrame orderPandas(DataFrame pandas) {
    return pandas.orderBy(pandas.col("pandaSize").asc(), pandas.col("age").desc());
  }

  public static DataFrame computeRelativePandaSizes(DataFrame pandas) {
    //tag::relativePandaSizesWindow[]
    WindowSpec windowSpec = Window
      .orderBy(pandas.col("age"))
      .partitionBy(pandas.col("zip"))
      .rowsBetween(-10, 10); // can use rangeBetween for range instead
    //end::relativePandaSizesWindow[]

    //tag::relativePandaSizesQuery[]
    Column pandaRelativeSizeCol = pandas.col("pandaSize").minus(avg(pandas.col("pandaSize")).over(windowSpec));

    return pandas.select(pandas.col("name"), pandas.col("zip"), pandas.col("pandaSize"),
      pandas.col("age"), pandaRelativeSizeCol.as("panda_relative_size"));
    //end::relativePandaSizesQuery[]
  }

  public static void joins(DataFrame df1, DataFrame df2) {
    //tag::innerJoin[]
    // Inner join implicit
    df1.join(df2, df1.col("name").equalTo(df2.col("name")));
    // Inner join explicit
    df1.join(df2, df1.col("name").equalTo(df2.col("name")), "inner");
    //end::innerJoin[]

    //tag::leftouterJoin[]
    // Left outer join explicit
    df1.join(df2, df1.col("name").equalTo(df2.col("name")), "left_outer");
    //end::leftouterJoin[]

    //tag::rightouterJoin[]
    // Right outer join explicit
    df1.join(df2, df1.col("name").equalTo(df2.col("name")), "right_outer");
    //end::rightouterJoin[]

    //tag::leftsemiJoin[]
    // Left semi join explicit
    df1.join(df2, df1.col("name").equalTo(df2.col("name")), "leftsemi");
    //end::leftsemiJoin[]
  }

  public static DataFrame selfJoin(DataFrame df) {
    return df.as("a").join(df.as("b")).where(df.col("name").equalTo(df.col("name")));
  }

}
