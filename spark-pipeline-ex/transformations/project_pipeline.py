# Spark Declarative Pipelines example
# Demonstrates a diamond-shaped DAG with branching:
#
#   raw_projects (temporary view — CSV ingest, not persisted)
#            /            \
#   popular_projects   creator_stats
#            \            /
#         project_summary (join)

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count

spark = SparkSession.active()


# tag::temp_view_ingest[]
@dp.temporary_view
def raw_projects() -> DataFrame:
    """Ingest raw project data from CSV.

    Uses @dp.temporary_view because raw ingestion data is intermediate —
    it does not need to be persisted, only consumed by downstream views.
    """
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv("../data/project.csv")
    )


# end::temp_view_ingest[]


# tag::branch_filter[]
@dp.materialized_view
def popular_projects() -> DataFrame:
    """Filter to projects with more than 10 stars."""
    return spark.table("raw_projects").filter(col("stars") > 10)


# end::branch_filter[]


# tag::branch_agg[]
@dp.materialized_view
def creator_stats() -> DataFrame:
    """Aggregate stats per creator."""
    return spark.table("raw_projects").groupBy("creator").agg(
        count("*").alias("num_projects"),
        avg("stars").alias("avg_stars"),
    )
# end::branch_agg[]
# tag::join[]
@dp.materialized_view
def project_summary() -> DataFrame:
    """Join popular projects with their creator stats."""
    return (
        spark.table("popular_projects")
        .join(spark.table("creator_stats"), on="creator")
        .select("creator", "projectname", "stars", "num_projects", "avg_stars")
    )


# end::join[]
