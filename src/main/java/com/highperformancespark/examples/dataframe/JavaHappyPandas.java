package com.highperformancespark.examples.dataframe;

import com.highperformancespark.examples.objects.JavaRawPanda;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;
import scala.reflect.ClassTag$;
import scala.reflect.api.TypeTags;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.reflect.runtime.*;

import static org.apache.spark.sql.functions.*;

public class JavaHappyPandas {

  /**
   * Creates SQLContext with an existing SparkContext.
   */
  public SQLContext sqlContext(JavaSparkContext jsc) {
    SQLContext sqlContext = new SQLContext(jsc);
    return sqlContext;
  }

  /**
   * Creates HiveContext with an existing SparkContext.
   */
  public HiveContext hiveContext(JavaSparkContext jsc) {
    HiveContext hiveContext = new HiveContext(jsc);
    return hiveContext;
  }

  /**
   * Illustrate loading some JSON data.
   */
  public DataFrame loadDataSimple(JavaSparkContext jsc, SQLContext sqlContext, String path) {
    DataFrame df1 = sqlContext.read().json(path);

    DataFrame df2 = sqlContext.read().format("json").option("samplingRatio", "1.0").load(path);

    JavaRDD<String> jsonRDD = jsc.textFile(path);
    DataFrame df3 = sqlContext.read().json(jsonRDD);

    return df1;
  }

  public DataFrame jsonLoadFromRDD(SQLContext sqlContext, JavaRDD<String> input) {
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
  public DataFrame happyPandasPercentage(DataFrame pandaInfo) {
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
  public DataFrame encodePandaType(DataFrame pandaInfo) {
    DataFrame encodedDF = pandaInfo.select(pandaInfo.col("id"),
        when(pandaInfo.col("pt").equalTo("giant"), 0).
        when(pandaInfo.col("pt").equalTo("red"), 1).
        otherwise(2).as("encodedType"));

    return encodedDF;
  }

  /**
   * Gets places with happy pandas more than minHappinessBound.
   */
  public DataFrame minHappyPandas(DataFrame pandaInfo, int minHappyPandas) {
    return pandaInfo.filter(pandaInfo.col("happyPandas").geq(minHappyPandas));
  }

  /**
   * Extra the panda info from panda places and compute the squisheness of the panda
   */
  public DataFrame squishPandaFromPace(DataFrame pandaPlace) {
    Buffer<Column> inputCols = JavaConversions.asScalaBuffer(Arrays.asList(pandaPlace.col("pandas")));

    TypeTags.TypeTag tag = null; // TODO don't know how to create Type Tag in java ??

    DataFrame pandaInfo = pandaPlace.explode(inputCols.toList(), r -> {
      List<Row> pandas = r.getList(0);
      List<JavaRawPanda> rawPandasList = pandas
        .stream()
        .map(a -> {
          long id = a.getLong(0);
          String zip = a.getString(1);
          String pt = a.getString(2);
          boolean happy = a.getBoolean(3);
          List<Double> attrs = a.getList(4);
          return new JavaRawPanda(id, zip, pt, happy, attrs);
        }).collect(Collectors.toList());

      return JavaConversions.asScalaBuffer(rawPandasList);
    }, tag);

    DataFrame squishyness =
      pandaInfo.select((pandaInfo.col("attributes").apply(0).divide(pandaInfo.col("attributes")).apply(1))
        .as("squishyness"));

    return squishyness;
  }

  /**
   * Find pandas that are sad.
   */
  public DataFrame sadPandas(DataFrame pandaInfo) {
    return pandaInfo.filter(pandaInfo.col("happy").notEqual(true));
  }

  /**
   * Find pandas that are happy and fuzzier than squishy.
   */
  public DataFrame happyFuzzyPandas(DataFrame pandaInfo) {
    DataFrame df = pandaInfo.filter(
      pandaInfo.col("happy").and(pandaInfo.col("attributes").apply(0)).gt(pandaInfo.col("attributes").apply(1))
    );

    return df;
  }

  /**
   * Gets places that contains happy pandas more than unhappy pandas.
   */
  public DataFrame happyPandasPlaces(DataFrame pandaInfo) {
    return pandaInfo.filter(pandaInfo.col("happyPandas").geq(pandaInfo.col("totalPandas").divide(2)));
  }

  /**
   * Remove duplicate pandas by id.
   */
  public DataFrame removeDuplicates(DataFrame pandas) {
    DataFrame df = pandas.dropDuplicates(new String[]{"id"});
    return df;
  }

  public DataFrame describePandas(DataFrame pandas) {
    return pandas.describe();
  }

  public DataFrame maxPandaSizePerZip(DataFrame pandas) {
    return pandas.groupBy(pandas.col("zip")).max("pandaSize");
  }

  public DataFrame minMaxPandaSizePerZip(DataFrame pandas) {
    return pandas.groupBy(pandas.col("zip")).agg(min("pandaSize"), max("pandaSize"));
  }

  public DataFrame minPandaSizeMaxAgePerZip(DataFrame pandas) {
    Map<String, String> map = new HashMap<>();
    map.put("pandaSize", "min");
    map.put("age", "max");

    DataFrame df = pandas.groupBy(pandas.col("zip")).agg(map);
    return df;
  }

  public DataFrame minMeanSizePerZip(DataFrame pandas) {
    return pandas.groupBy(pandas.col("zip")).agg(min(pandas.col("pandaSize")), mean(pandas.col("pandaSize")));
  }

  public DataFrame simpleSqlExample(DataFrame pandas) {
    SQLContext sqlContext = pandas.sqlContext();
    pandas.registerTempTable("pandas");

    DataFrame miniPandas = sqlContext.sql("SELECT * FROM pandas WHERE pandaSize < 12");
    return miniPandas;
  }

  public void startJDBCServer(HiveContext sqlContext) {
    sqlContext.setConf("hive.server2.thrift.port", "9090");
    HiveThriftServer2.startWithContext(sqlContext);
  }

  /**
   * Orders pandas by size ascending and by age descending.
   * Pandas will be sorted by "size" first and if two pandas have the same "size"
   * will be sorted by "age".
   */
  public DataFrame orderPandas(DataFrame pandas) {
    return pandas.orderBy(pandas.col("pandaSize").asc(), pandas.col("age").desc());
  }

  public DataFrame computeRelativePandaSizes(DataFrame pandas) {
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

  public void joins(DataFrame df1, DataFrame df2) {
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

  public DataFrame selfJoin(DataFrame df) {
    return df.as("a").join(df.as("b")).where(df.col("name").equalTo(df.col("name")));
  }

  class JavaPandaInfo {
    private String place;
    private String pandaType;
    private int happyPandas;
    private int totalPandas;

    /**
     * @param place       name of place
     * @param pandaType   type of pandas in this place
     * @param happyPandas number of happy pandas in this place
     * @param totalPandas total number of pandas in this place
     */
    public JavaPandaInfo(String place, String pandaType, int happyPandas, int totalPandas) {
      this.place = place;
      this.pandaType = pandaType;
      this.happyPandas = happyPandas;
      this.totalPandas = totalPandas;
    }

    public String getPlace() {
      return place;
    }

    public void setPlace(String place) {
      this.place = place;
    }

    public String getPandaType() {
      return pandaType;
    }

    public void setPandaType(String pandaType) {
      this.pandaType = pandaType;
    }

    public int getHappyPandas() {
      return happyPandas;
    }

    public void setHappyPandas(int happyPandas) {
      this.happyPandas = happyPandas;
    }

    public int getTotalPandas() {
      return totalPandas;
    }

    public void setTotalPandas(int totalPandas) {
      this.totalPandas = totalPandas;
    }

  }

  class JavaPandas {
    private String name;
    private String zip;
    private int pandaSize;
    private int age;

    /**
     * @param name name of panda
     * @param zip zip code
     * @param pandaSize size of panda in KG
     * @param age age of panda
     */
    public JavaPandas(String name, String zip, int pandaSize, int age) {
      this.name = name;
      this.zip = zip;
      this.pandaSize = pandaSize;
      this.age = age;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getZip() {
      return zip;
    }

    public void setZip(String zip) {
      this.zip = zip;
    }

    public int getPandaSize() {
      return pandaSize;
    }

    public void setPandaSize(int pandaSize) {
      this.pandaSize = pandaSize;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }

  }

}
