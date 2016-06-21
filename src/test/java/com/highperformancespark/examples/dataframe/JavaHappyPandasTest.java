package com.highperformancespark.examples.dataframe;

import com.highperformancespark.examples.objects.JavaPandaInfo;
import com.highperformancespark.examples.objects.JavaPandas;
import com.highperformancespark.examples.objects.JavaRawPanda;
import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class JavaHappyPandasTest extends JavaDataFrameSuiteBase {
  String toronto = "toronto";
  String sandiego = "san diego";
  String virginia = "virginia";

  List<JavaPandaInfo> pandaInfoList = Arrays.asList(
    new JavaPandaInfo(toronto, "giant", 1, 2),
    new JavaPandaInfo(sandiego, "red", 2, 3),
    new JavaPandaInfo(virginia, "black", 1, 10)
  );

  List<JavaRawPanda> rawPandaList = Arrays.asList(
    new JavaRawPanda(10L, "94110", "giant", true, Arrays.asList(1.0, 0.9)),
    new JavaRawPanda(11L, "94110", "red", true, Arrays.asList(1.0, 0.9)));

  List<JavaPandas> pandasList = Arrays.asList(
    new JavaPandas("bata", "10010", 10, 2),
    new JavaPandas("wiza", "10010", 20, 4),
    new JavaPandas("dabdob", "11000", 8, 2),
    new JavaPandas("hanafy", "11000", 15, 7),
    new JavaPandas("hamdi", "11111", 20, 10)
  );

  @Test
  public void simpleSelfJoinTest() {
    Dataset<Row> inputDF = sqlContext().createDataFrame(pandasList, JavaPandas.class);
    Dataset<Row> result = JavaHappyPandas.selfJoin(inputDF).select("a.name", "b.name");
    List<Row> resultList = result.collectAsList();

    resultList.stream().forEach(row -> assertEquals(row.getString(0), row.getString(1)));
  }

  @Test
  public void verifyhappyPandasPercentage() {
    List<Row> expectedList = Arrays.asList(RowFactory.create(toronto, 0.5),
      RowFactory.create(sandiego, 2 / 3.0), RowFactory.create(virginia, 1/10.0));
    Dataset<Row> expectedDF = sqlContext().createDataFrame(
      expectedList, new StructType(
        new StructField[]{
          new StructField("place", DataTypes.StringType, true, Metadata.empty()),
          new StructField("percentHappy", DataTypes.DoubleType, true, Metadata.empty())
        }));

    Dataset<Row> inputDF = sqlContext().createDataFrame(pandaInfoList, JavaPandaInfo.class);
    Dataset<Row> resultDF = JavaHappyPandas.happyPandasPercentage(inputDF);

    assertDataFrameApproximateEquals(expectedDF, resultDF, 1E-5);
  }

  @Test
  public void encodePandaType() {
    Dataset<Row> inputDF = sqlContext().createDataFrame(rawPandaList, JavaRawPanda.class);
    Dataset<Row> resultDF = JavaHappyPandas.encodePandaType(inputDF);

    List<Row> expectedRows = Arrays.asList(RowFactory.create(10L, 0), RowFactory.create(11L, 1));
    Dataset<Row> expectedDF = sqlContext().createDataFrame(expectedRows, new StructType(new StructField[]{
      new StructField("id", DataTypes.LongType, false, Metadata.empty()),
      new StructField("encodedType", DataTypes.IntegerType, false, Metadata.empty())
    }));

    assertDataFrameEquals(expectedDF, resultDF);
  }

  @Test
  public void happyPandasPlaces() {
    Dataset<Row> inputDF = sqlContext().createDataFrame(pandaInfoList, JavaPandaInfo.class);
    Dataset<Row> resultDF = JavaHappyPandas.happyPandasPlaces(inputDF);

    List<JavaPandaInfo> expectedRows = Arrays.asList(
      new JavaPandaInfo(toronto, "giant", 1, 2),
      new JavaPandaInfo(sandiego, "red", 2, 3));
    Dataset<Row> expectedDF = sqlContext().createDataFrame(expectedRows, JavaPandaInfo.class);

    assertDataFrameEquals(expectedDF, resultDF);
  }

  @Test
  public void maxPandaSizePerZip() {
    Dataset<Row> inputDF = sqlContext().createDataFrame(pandasList, JavaPandas.class);
    Dataset<Row> resultDF = JavaHappyPandas.maxPandaSizePerZip(inputDF);

    List<Row> expectedRows = Arrays.asList(
      RowFactory.create(pandasList.get(1).getZip(), pandasList.get(1).getPandaSize()),
      RowFactory.create(pandasList.get(3).getZip(), pandasList.get(3).getPandaSize()),
      RowFactory.create(pandasList.get(4).getZip(), pandasList.get(4).getPandaSize())
    );
    Dataset<Row> expectedDF = sqlContext().createDataFrame(expectedRows,
      new StructType(
        new StructField[]{
          new StructField("zip", DataTypes.StringType, true, Metadata.empty()),
          new StructField("max(pandaSize)", DataTypes.IntegerType, true, Metadata.empty())
        }
      ));

    assertDataFrameEquals(expectedDF.orderBy("zip"), resultDF.orderBy("zip"));
  }

  @Test
  public void complexAggPerZip() {
    Dataset<Row> inputDF = sqlContext().createDataFrame(pandasList, JavaPandas.class);
    Dataset<Row> resultDF = JavaHappyPandas.minMeanSizePerZip(inputDF);

    List<Row> expectedRows = Arrays.asList(
      RowFactory.create(pandasList.get(1).getZip(), pandasList.get(0).getPandaSize(), 15.0),
      RowFactory.create(pandasList.get(3).getZip(), pandasList.get(2).getPandaSize(), 11.5),
      RowFactory.create(pandasList.get(4).getZip(), pandasList.get(4).getPandaSize(), 20.0));

    Dataset<Row> expectedDF = sqlContext().createDataFrame(expectedRows,
      new StructType(
        new StructField[]{
          new StructField("zip", DataTypes.StringType, true, Metadata.empty()),
          new StructField("min(pandaSize)", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("avg(pandaSize)", DataTypes.DoubleType, true, Metadata.empty())
        }
      ));

    assertDataFrameApproximateEquals(expectedDF.orderBy("zip"), resultDF.orderBy("zip"), 1E-5);
  }

  @Test
  public void simpleSQLExample() {
    Dataset<Row> inputDF = sqlContext().createDataFrame(pandasList, JavaPandas.class);
    Dataset<Row> resultDF = JavaHappyPandas.simpleSqlExample(inputDF);

    List<JavaPandas> expectedList = Arrays.asList(
      pandasList.get(0), pandasList.get(2)
    );
    Dataset<Row> expectedDF = sqlContext().createDataFrame(expectedList, JavaPandas.class);

    assertDataFrameEquals(expectedDF, resultDF);
  }

}
