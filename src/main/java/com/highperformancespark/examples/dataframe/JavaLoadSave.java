package com.highperformancespark.examples.dataframe;

import com.highperformancespark.examples.objects.JavaPandaPlace;
import com.highperformancespark.examples.objects.JavaRawPanda;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class JavaLoadSave {
  private SQLContext sqlContext;

  public JavaLoadSave(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  //tag::createFromRDD[]
  public Dataset<Row> createFromJavaBean(JavaRDD<JavaPandaPlace> input) {
    // Create DataFrame using Java Bean
    Dataset<Row> df1 = sqlContext.createDataFrame(input, JavaPandaPlace.class);

    // Create DataFrame using JavaRDD<Row>
    JavaRDD<Row> rowRDD = input.map(pm -> RowFactory.create(pm.getName(),
      pm.getPandas().stream()
      .map(pi -> RowFactory.create(pi.getId(), pi.getZip(), pi.isHappy(), pi.getAttributes()))
      .collect(Collectors.toList())));

    ArrayType pandasType = DataTypes.createArrayType(new StructType(
      new StructField[]{
        new StructField("id", DataTypes.LongType, true, Metadata.empty()),
        new StructField("zip", DataTypes.StringType, true, Metadata.empty()),
        new StructField("happy", DataTypes.BooleanType, true, Metadata.empty()),
        new StructField("attributes", DataTypes.createArrayType(DataTypes.FloatType), true, Metadata.empty())
      }
    ));

    StructType schema = new StructType(new StructField[]{
      new StructField("name", DataTypes.StringType, true, Metadata.empty()),
      new StructField("pandas", pandasType, true, Metadata.empty())
    });

    Dataset<Row> df2 = sqlContext.createDataFrame(rowRDD, schema);
    return df2;
  }
  //end::createFromRDD[]

  //tag::createFromLocal[]
  public Dataset<Row> createFromLocal(List<PandaPlace> input) {
    return sqlContext.createDataFrame(input, PandaPlace.class);
  }
  //end::createFromLocal[]

  //tag::collectResults[]
  public List<Row> collectDF(Dataset<Row> df) {
    return df.collectAsList();
  }
  //end::collectResults[]

  //tag::toRDD[]
  public JavaRDD<JavaRawPanda> toRDD(Dataset<Row> input) {
    JavaRDD<JavaRawPanda> rdd = input.javaRDD().map(row -> new JavaRawPanda(row.getLong(0), row.getString(1),
      row.getString(2), row.getBoolean(3), row.getList(4)));
    return rdd;
  }
  //end::toRDD[]

  //tag::partitionedOutput[]
  public void writeOutByZip(Dataset<Row> input) {
    input.write().partitionBy("zipcode").format("json").save("output/");
  }
  //end::partitionedOutput[]

  //tag::saveAppend[]
  public void writeAppend(Dataset<Row> input) {
    input.write().mode(SaveMode.Append).save("output/");
  }
  //end::saveAppend[]

  public Dataset<Row> createJDBC() {
    //tag::createJDBC[]
    Dataset<Row> df1 = sqlContext.read().jdbc("jdbc:dialect:serverName;user=user;password=pass",
      "table", new Properties());

    Dataset<Row> df2 = sqlContext.read().format("jdbc")
      .option("url", "jdbc:dialect:serverName")
      .option("dbtable", "table").load();

    return df2;
    //end::createJDBC[]
  }

  public void writeJDBC(Dataset<Row> df) {
    //tag::writeJDBC[]
    df.write().jdbc("jdbc:dialect:serverName;user=user;password=pass",
      "table", new Properties());

    df.write().format("jdbc")
      .option("url", "jdbc:dialect:serverName")
      .option("user", "user")
      .option("password", "pass")
      .option("dbtable", "table").save();
    //end::writeJDBC[]
  }

  //tag::loadParquet[]
  public Dataset<Row> loadParquet(String path) {
    // Configure Spark to read binary data as string, note: must be configured on SQLContext
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true");

    // Load parquet data using merge schema (configured through option)
    Dataset<Row> df = sqlContext.read()
      .option("mergeSchema", "true")
      .format("parquet")
      .load(path);

    return df;
  }
  //end::loadParquet[]

  //tag::writeParquet[]
  public void writeParquet(Dataset<Row> df, String path) {
    df.write().format("parquet").save(path);
  }
  //end::writeParquet[]

  //tag::loadHiveTable[]
  public Dataset<Row> loadHiveTable() {
    return sqlContext.read().table("pandas");
  }
  //end::loadHiveTable[]

  //tag::saveManagedTable[]
  public void saveManagedTable(Dataset<Row> df) {
    df.write().saveAsTable("pandas");
  }
  //end::saveManagedTable[]
}
