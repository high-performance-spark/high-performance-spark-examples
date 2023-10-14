# When running this example make sure to include the built Scala jar :
#
# $SPARK_HOME/bin/pyspark --jars \
# ./target/examples-0.0.1.jar --driver-class-path ./target/examples-0.0.1.jar
#
# This example illustrates how to interface Scala and Python code, but caution
# should be taken as it depends on many private members that may change in
# future releases of Spark.

from pyspark.sql.types import StructType, IntegerType, DoubleType, StructField
from pyspark.sql import DataFrame, SparkSession
import sys
import timeit
import time


def generate_scale_data(sqlCtx, rows, numCols):
    """
    Generate scale data for the performance test.

    This also illustrates calling custom Scala code from the driver.

    .. Note: This depends on many internal methods and may break between versions.

    # This assumes our jars have been added with export PYSPARK_SUBMIT_ARGS
    >>> session = SparkSession.builder.getOrCreate()
    >>> scaleData = generate_scale_data(session, 100L, 1)
    >>> scaleData[0].count()
    100
    >>> scaleData[1].count()
    100
    >>> session.stop()
    """
    # tag::javaInterop[]
    sc = sqlCtx._sc
    javaSparkSession = sqlCtx._jsparkSession
    jsc = sc._jsc
    scalasc = jsc.sc()
    gateway = sc._gateway
    # Call a java method that gives us back an RDD of JVM Rows (Int, Double)
    # While Python RDDs are wrapped Java RDDs (even of Rows) the contents are
    # different, so we can't directly wrap this.
    # This returns a Java RDD of Rows - normally it would better to
    # return a DataFrame directly, but for illustration we will work
    # with an RDD of Rows.
    java_rdd = gateway.jvm.com.highperformancespark.examples.tools.GenerateScalingData.generateMiniScaleRows(
        scalasc, rows, numCols
    )
    # Schemas are serialized to JSON and sent back and forth
    # Construct a Python Schema and turn it into a Java Schema
    schema = StructType(
        [StructField("zip", IntegerType()), StructField("fuzzyness", DoubleType())]
    )
    jschema = javaSparkSession.parseDataType(schema.json())
    # Convert the Java RDD to Java DataFrame
    java_dataframe = javaSparkSession.createDataFrame(java_rdd, jschema)
    # Wrap the Java DataFrame into a Python DataFrame
    python_dataframe = DataFrame(java_dataframe, sqlCtx)
    # Convert the Python DataFrame into an RDD
    pairRDD = python_dataframe.rdd.map(lambda row: (row[0], row[1]))
    return (python_dataframe, pairRDD)
    # end::javaInterop[]


def runOnDF(df):
    result = df.groupBy("zip").avg("fuzzyness").count()
    return result


def runOnRDD(rdd):
    result = (
        rdd.map(lambda x, y: (x, (y, 1)))
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        .count()
    )
    return result


def groupOnRDD(rdd):
    return rdd.groupByKey().mapValues(lambda v: sum(v) / float(len(v))).count()


def run(sc, sqlCtx, scalingFactor, size):
    """
    Run the simple perf test printing the results to stdout.

    >>> session = SparkSession.builder.getOrCreate()
    >>> sc = session._sc
    >>> run(sc, session, 5L, 1)
    RDD:
    ...
    group:
    ...
    df:
    ...
    yay
    >>> session.stop()
    """
    (input_df, input_rdd) = generate_scale_data(sqlCtx, scalingFactor, size)
    input_rdd.cache().count()
    rddTimeings = timeit.repeat(
        stmt=lambda: runOnRDD(input_rdd),
        repeat=10,
        number=1,
        timer=time.time,
        setup="gc.enable()",
    )
    groupTimeings = timeit.repeat(
        stmt=lambda: groupOnRDD(input_rdd),
        repeat=10,
        number=1,
        timer=time.time,
        setup="gc.enable()",
    )
    input_df.cache().count()
    dfTimeings = timeit.repeat(
        stmt=lambda: runOnDF(input_df),
        repeat=10,
        number=1,
        timer=time.time,
        setup="gc.enable()",
    )
    print(f"RDD: {rddTimeings}, group: {groupTimeings}, df: {dfTimeings}")


def parseArgs(args):
    """
    Parse the args, no error checking.

    >>> parseArgs(["foobaz", "1", "2"])
    (1, 2)
    """
    scalingFactor = int(args[1])
    size = int(args[2])
    return (scalingFactor, size)


if __name__ == "__main__":
    """
    Usage: simple_perf_test scalingFactor size
    """
    
    scalingFactor = 1
    size = 1
    if len(sys.argv) > 2:
        (scalingFactor, size) = parseArgs(sys.argv)
    session = SparkSession.builder.appName("SimplePythonPerf").getOrCreate()
    sc = session._sc
    run(sc, session, scalingFactor, size)

    sc.stop()
