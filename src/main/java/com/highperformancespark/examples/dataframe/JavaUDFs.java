package com.highperformancespark.examples.dataframe;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;

public class JavaUDFs {

  public static void setupUDFs(SQLContext sqlContext) {
    //tag::basicUDF[]
    sqlContext.udf()
      .register("strlen",
                (String s) -> s.length(), DataTypes.StringType);
    //end::basicUDF[]
  }

  public static void setupUDAFs(SQLContext sqlContext) {

    class Avg extends UserDefinedAggregateFunction {

      @Override
      public StructType inputSchema() {
        StructType inputSchema =
          new StructType(new StructField[]{new StructField("value", DataTypes.DoubleType, true, Metadata.empty())});
        return inputSchema;
      }

      @Override
      public StructType bufferSchema() {
        StructType bufferSchema =
          new StructType(new StructField[]{
            new StructField("count", DataTypes.LongType, true, Metadata.empty()),
            new StructField("sum", DataTypes.DoubleType, true, Metadata.empty())
          });

        return bufferSchema;
      }

      @Override
      public DataType dataType() {
        return DataTypes.DoubleType;
      }

      @Override
      public boolean deterministic() {
        return true;
      }

      @Override
      public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0L);
        buffer.update(1, 0.0);
      }

      @Override
      public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0, buffer.getLong(0) + 1);
        buffer.update(1, buffer.getDouble(1) + input.getDouble(0));
      }

      @Override
      public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0));
        buffer1.update(1, buffer1.getDouble(1) + buffer2.getDouble(1));
      }

      @Override
      public Object evaluate(Row buffer) {
        return buffer.getDouble(1) / buffer.getLong(0);
      }
    }

    Avg average = new Avg();
    sqlContext.udf().register("ourAvg", average);
  }
}
