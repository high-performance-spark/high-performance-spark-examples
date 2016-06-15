package com.highperformancespark.examples.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.*;
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
  public static Dataset<Row> loadDataSimple(JavaSparkContext jsc, SQLContext sqlContext, String path) {
    Dataset<Row> df1 = sqlContext.read().json(path);

    Dataset<Row> df2 = sqlContext.read().format("json").option("samplingRatio", "1.0").load(path);

    JavaRDD<String> jsonRDD = jsc.textFile(path);
    Dataset<Row> df3 = sqlContext.read().json(jsonRDD);

    return df1;
  }

  public static Dataset<Row> jsonLoadFromRDD(SQLContext sqlContext, JavaRDD<String> input) {
    JavaRDD<String> rdd = input.filter(e -> e.contains("panda"));
    Dataset<Row> df = sqlContext.read().json(rdd);
    return df;
  }

  //  Here will be some examples on PandaInfo DataFrame

  /**
   * Gets the percentage of happy pandas per place.
   *
   * @param pandaInfo the input DataFrame
   * @return Returns DataFrame of (place, percentage of happy pandas)
   */
  public static Dataset<Row> happyPandasPercentage(Dataset<Row> pandaInfo) {
    Dataset<Row> happyPercentage = pandaInfo.select(pandaInfo.col("place"),
      (pandaInfo.col("happyPandas").divide(pandaInfo.col("totalPandas"))).as("percentHappy"));
    return happyPercentage;
  }

  /**
   * Encodes pandaType to Integer values instead of String values.
   *
   * @param pandaInfo the input DataFrame
   * @return Returns a DataFrame of pandaId and integer value for pandaType.
   */
  public static Dataset<Row> encodePandaType(Dataset<Row> pandaInfo) {
    Dataset<Row> encodedDF = pandaInfo.select(pandaInfo.col("id"),
        when(pandaInfo.col("pt").equalTo("giant"), 0).
        when(pandaInfo.col("pt").equalTo("red"), 1).
        otherwise(2).as("encodedType"));

    return encodedDF;
  }

  /**
   * Gets places with happy pandas more than minHappinessBound.
   */
  public static Dataset<Row> minHappyPandas(Dataset<Row> pandaInfo, int minHappyPandas) {
    return pandaInfo.filter(pandaInfo.col("happyPandas").geq(minHappyPandas));
  }

  /**
   * Find pandas that are sad.
   */
  public static Dataset<Row> sadPandas(Dataset<Row> pandaInfo) {
    return pandaInfo.filter(pandaInfo.col("happy").notEqual(true));
  }

  /**
   * Find pandas that are happy and fuzzier than squishy.
   */
  public static Dataset<Row> happyFuzzyPandas(Dataset<Row> pandaInfo) {
    Dataset<Row> df = pandaInfo.filter(
      pandaInfo.col("happy").and(pandaInfo.col("attributes").apply(0)).gt(pandaInfo.col("attributes").apply(1))
    );

    return df;
  }

  /**
   * Gets places that contains happy pandas more than unhappy pandas.
   */
  public static Dataset<Row> happyPandasPlaces(Dataset<Row> pandaInfo) {
    return pandaInfo.filter(pandaInfo.col("happyPandas").geq(pandaInfo.col("totalPandas").divide(2)));
  }

  /**
   * Remove duplicate pandas by id.
   */
  public static Dataset<Row> removeDuplicates(Dataset<Row> pandas) {
    Dataset<Row> df = pandas.dropDuplicates(new String[]{"id"});
    return df;
  }

  public static Dataset<Row> describePandas(Dataset<Row> pandas) {
    return pandas.describe();
  }

  public static Dataset<Row> maxPandaSizePerZip(Dataset<Row> pandas) {
    return pandas.groupBy(pandas.col("zip")).max("pandaSize");
  }

  public static Dataset<Row> minMaxPandaSizePerZip(Dataset<Row> pandas) {
    return pandas.groupBy(pandas.col("zip")).agg(min("pandaSize"), max("pandaSize"));
  }

  public static Dataset<Row> minPandaSizeMaxAgePerZip(Dataset<Row> pandas) {
    Map<String, String> map = new HashMap<>();
    map.put("pandaSize", "min");
    map.put("age", "max");

    Dataset<Row> df = pandas.groupBy(pandas.col("zip")).agg(map);
    return df;
  }

  public static Dataset<Row> minMeanSizePerZip(Dataset<Row> pandas) {
    return pandas.groupBy(pandas.col("zip")).agg(min(pandas.col("pandaSize")), mean(pandas.col("pandaSize")));
  }

  public static Dataset<Row> simpleSqlExample(Dataset<Row> pandas) {
    SQLContext sqlContext = pandas.sqlContext();
    pandas.registerTempTable("pandas");

    Dataset<Row> miniPandas = sqlContext.sql("SELECT * FROM pandas WHERE pandaSize < 12");
    return miniPandas;
  }

  /**
   * Orders pandas by size ascending and by age descending.
   * Pandas will be sorted by "size" first and if two pandas
   * have the same "size"  will be sorted by "age".
   */
  public static Dataset<Row> orderPandas(Dataset<Row> pandas) {
    return pandas.orderBy(pandas.col("pandaSize").asc(), pandas.col("age").desc());
  }

  public static Dataset<Row> computeRelativePandaSizes(Dataset<Row> pandas) {
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

  public static void joins(Dataset<Row> df1, Dataset<Row> df2) {
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
    df1.join(df2, df1.col("name").equalTo(df2.col("name")), "left_semi");
    //end::leftsemiJoin[]
  }

  public static Dataset<Row> selfJoin(Dataset<Row> df) {
    return (df.as("a")).join(df.as("b")).where("a.name = b.name");
  }

}
