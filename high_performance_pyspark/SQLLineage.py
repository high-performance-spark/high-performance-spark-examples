"""
>>> from pyspark.context import SparkContext
>>> from pyspark.sql import SQLContext, Row, DataFrame
>>> sc = SparkContext('local', 'test')
...
>>> sc.setLogLevel("ERROR")
>>> sqlCtx = SQLContext(sc)
...
>>> rdd = sc.parallelize(range(1, 100)).map(lambda x: Row(i = x))
>>> df = rdd.toDF()
>>> df2 = cutLineage(df)
>>> df.head() == df2.head()
True
>>> df.schema == df2.schema
True
"""

from pyspark.sql import DataFrame

# tag::cutLineage[]
def cutLineage(df):
    """
    Cut the lineage of a DataFrame - used for iterative algorithms
    
    .. Note: This uses internal members and may break between versions
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
