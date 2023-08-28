"""
>>> df = rdd.toDF()
>>> df2 = cutLineage(df)
>>> df.head() == df2.head()
True
>>> df.schema == df2.schema
True
"""

global df
global sc
global rdd
global spark

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Row
from pyspark.sql.session import SparkSession

# tag::cutLineage[]
def cutLineage(df):
    """
    Cut the lineage of a DataFrame - used for iterative algorithms

    .. Note: This uses internal members and may break between versions
    >>> df = rdd.toDF()
    >>> cutDf = cutLineage(df)
    >>> cutDf.count()
    3
    """
    jRDD = df._jdf.toJavaRDD()
    jSchema = df._jdf.schema()
    jRDD.cache()
    sqlCtx = df.sql_ctx
    try:
        javaSqlCtx = sqlCtx._jsqlContext
    except:
        javaSqlCtx = sqlCtx._ssql_ctx
    newJavaDF = javaSqlCtx.createDataFrame(jRDD, jSchema)
    newDF = DataFrame(newJavaDF, sqlCtx)
    return newDF


# end::cutLineage[]


def _setupTest():
    globs = globals()
    spark = SparkSession.builder.master("local[4]").getOrCreate()
    sc = spark._sc
    sc.setLogLevel("ERROR")
    globs["sc"] = sc
    globs["spark"] = spark
    globs["rdd"] = rdd = sc.parallelize(
        [
            Row(field1=1, field2="row1"),
            Row(field1=2, field2="row2"),
            Row(field1=3, field2="row3"),
        ]
    )
    return globs


def _test():
    """
    Run the tests.
    """
    import doctest

    globs = _setupTest()
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS
    )
    globs["sc"].stop()
    if failure_count:
        exit(-1)


import sys

if __name__ == "__main__":
    _test()
# Hack to support running in nose
elif sys.stdout != sys.__stdout__:
    _setupTest()
