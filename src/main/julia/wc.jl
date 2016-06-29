using Spark
sc = SparkContext(master="local")
path = string("file:///", ENV["SPARK_HOME"], "/README.md")
txt = text_file(sc, path)
# Normally we would use a flatmap, but currently only has map_partitions
