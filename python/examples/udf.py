# This script triggers a number of different PySpark errors

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import pandas_udf, udf
from typing import Iterator
import sys
import pandas as pd

global sc


# We need the session before we can use @udf
spark = SparkSession.builder.master("local[4]").getOrCreate()


# tag::simple_udf[]
@udf("long")
def classic_add1(e: int) -> int:
    return e + 1


# end::simple_udf[]


# tag::agg_new_udf[]
@pandas_udf("long")
def pandas_sum(s: pd.Series) -> int:
    return s.sum()


# end::agg_new_udf[]


# tag::new_udf[]
@pandas_udf("long")
def pandas_add1(s: pd.Series) -> pd.Series:
    # Vectorized operation on all of the elems in series at once
    return s + 1


# end::new_udf[]


# tag::complex_udf[]
@pandas_udf("long")
def pandas_nested_add1(d: pd.DataFrame) -> pd.Series:
    # Takes a struct and returns the age elem + 1, if we wanted
    # to update (e.g. return struct) we could update d and return it instead.
    return d["age"] + 1


# end::complex_udf[]


# tag::batches_of_batches_udf[]
@pandas_udf("long")
def pandas_batches_of_batches(t: Iterator[pd.Series]) -> Iterator[pd.Series]:
    my_db_connection = None  # Expensive setup logic goes here
    for s in t:
        # Do something with your setup logic
        if my_db_connection is None:
            # Vectorized operation on all of the elems in series at once
            yield s + 1


# end::batches_of_batches_udf[]


if __name__ == "__main__":
    # Make sure to make
    # "https://gender-pay-gap.service.gov.uk/viewing/download-data/2021"
    # available as ./data/2021
    uk_df = spark.read.csv("data/fetched/2021", header=True, inferSchema=True)
