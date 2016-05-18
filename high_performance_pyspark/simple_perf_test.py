# When running this example make sure to include the built Scala jar :
# $SPARK_HOME/bin/pyspark --jars ./target/examples-0.0.1.jar --driver-class-path ./target/examples-0.0.1.jar
# This example illustrates how to interface Scala and Python code, but caution
# should be taken as it depends on many private members that may change in
# future releases of Spark.

from pyspark.sql.types import *
from pyspark.sql import DataFrame
import timeit
import time

def generate_scale_data(sqlCtx, rows, numCols):
    """
    Generate scale data for the performance test.

    This also illustrates calling custom Scala code from the driver.

    .. Note: This depends on many internal methods and may break between versions.
    """
    sc = sqlCtx._sc
    # Get the SQL Context, 2.0 and pre-2.0 syntax
    try:
        javaSqlCtx = sqlCtx._jsqlContext
    except:
        javaSqlCtx = sqlCtx._ssql_ctx
    jsc = sc._jsc
    scalasc = jsc.sc()
    gateway = sc._gateway
    # Call a java method that gives us back an RDD of JVM Rows (Int, Double)
    # While Python RDDs are wrapped Java RDDs (even of Rows) the contents are different, so we
    # can't directly wrap this.
    # This returns a Java RDD of Rows - normally it would better to
    # return a DataFrame directly, but for illustration we will work with an RDD
    # of Rows.
    java_rdd = gateway.jvm.com.highperformancespark.examples.tools.GenerateScalingData. \
               generateMiniScaleRows(scalasc, rows, numCols)
    # Schemas are serialized to JSON and sent back and forth
    # Construct a Python Schema and turn it into a Java Schema
    schema = StructType([StructField("zip", IntegerType()), StructField("fuzzyness", DoubleType())])
    jschema = javaSqlCtx.parseDataType(schema.json())
    # Convert the Java RDD to Java DataFrame
    java_dataframe = javaSqlCtx.createDataFrame(java_rdd, jschema)
    # Wrap the Java DataFrame into a Python DataFrame
    python_dataframe = DataFrame(java_dataframe, sqlCtx)
    # Convert the Python DataFrame into an RDD
    pairRDD = python_dataframe.rdd.map(lambda row: (row[0], row[1]))
    return (python_dataframe, pairRDD)

def testOnDF(df):
    result = df.groupBy("zip").avg("fuzzyness").count()
    return result

def testOnRDD(rdd):
    result = rdd.map(lambda (x, y): (x, (y, 1))). \
             reduceByKey(lambda x, y: (x[0] + y [0], x[1] + y[1])). \
             count()
    return result

def groupOnRDD(rdd):
    return rdd.groupByKey().mapValues(lambda v: sum(v) / float(len(v))).count()

def run(sc, sqlCtx, scalingFactor, size):
    (input_df, input_rdd) = generate_scale_data(sqlCtx, scalingFactor, size)
    input_rdd.cache().count()
    rddTimeings = timeit.repeat(stmt=lambda: testOnRDD(input_rdd), repeat=10, number=1, timer=time.time)
    groupTimeings = timeit.repeat(stmt=lambda: groupOnRDD(input_rdd), repeat=10, number=1, timer=time.time)
    input_df.cache().count()
    dfTimeings = timeit.repeat(stmt=lambda: testOnDF(input_df), repeat=10, number=1, timer=time.time)
    print "RDD:"
    print rddTimeings
    print "group:"
    print groupTimeings
    print "df:"
    print dfTimeings
    print "yay"

if __name__ == "__main__":

    """
    Usage: simple_perf_test scalingFactor size
    """
    import sys
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    scalingFactor = int(sys.argv[1])
    size = int(sys.argv[2])
    sc = SparkContext(appName="SimplePythonPerf")
    sqlCtx = SQLContext(sc)
    run(sc, sqlCtx, scalingFactor, size)

    sc.stop()
