from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.active()

@dp.materialized_view
def example_python_materialized_view() -> DataFrame:
    return spark.range(10)
