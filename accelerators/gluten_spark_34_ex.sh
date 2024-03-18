#!/bin/bash

source setup.sh

SPARK_HOME=${SPARK_DIR}
export SPARK_HOME
PATH=$(pwd)/${SPARK_DIR}/bin:$PATH
spark-sql --master local[5] \
  --conf spark.plugins=io.glutenproject.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=5g \
  --jars ${GLUTEN_JAR} \
  -e "SELECT 1"
