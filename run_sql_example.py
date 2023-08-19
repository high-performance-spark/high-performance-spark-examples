import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help= "Input file",
                    required=True)
parser.add_argument("-d", "--delay", help = "Delay",
                    default=0,
                    required=False)
parser.add_argument("-m", "--master", help="Spark master",
                    default="local[*]")


args = parser.parse_args()


session = sparkSession.builder().appName(f"sql {args.input}").master(args.master).getOrCreate()

with open(args.input) as fr:
   query = fr.read()
results = session.sql(query)
results.count()
results.show()


if args.delay is not None:
    import time
    time.sleep(args.delay)
