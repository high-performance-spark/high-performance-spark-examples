import random
import string
from pyspark import SparkFiles
from pyspark.sql import *
from spark_expectations.core.expectations import SparkExpectations

spark = SparkSession.builder.master("local[4]").getOrCreate()
sc = spark.sparkContext

spark.sql("DROP TABLE IF EXISTS local.large_target")
spark.sql("CREATE TABLE local.large_target (junk STRING)")
spark.sql("SELECT * FROM local.large_target").count()
spark.sql(
    "ALTER TABLE local.large_target SET TBLPROPERTIES ('write.target-file-size-bytes'='5368709120')"
)
spark.sql("SELECT * FROM local.large_target").count()


def generate_random(N: int) -> str:
    return ["".join(random.choices(string.ascii_uppercase + string.digits, k=N))]


rdd = sc.parallelize(range(536870910, 536871210))
junk = rdd.map(generate_random)
junk_df = spark.createDataFrame(junk, ["junk"]).repartition(1)
junk_df.write.mode("append").saveAsTable("local.large_target")

# Can we read it back?

print(spark.sql("SELECT * FROM local.large_target").count())
