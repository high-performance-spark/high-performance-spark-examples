#!/bin/bash

# Check if we gluten and gluten UDFs present
GLUTEN_NATIVE_LIB_NAME=libhigh-performance-spark-gluten-0.so
NATIVE_LIB_DIR=$(pwd)/../native/src/
NATIVE_LIB_PATH="${NATIVE_LIB_DIR}${GLUTEN_NATIVE_LIB_NAME}"
GLUTEN_HOME=incubator-gluten
source /etc/lsb-release
if [ -n "$GLUTEN_JAR_PATH" ]; then
  GLUTEN_EXISTS="true"
  GLUTEN_SPARK_EXTRA="--conf spark.plugins=io.glutenproject.GlutenPlugin \
     --conf spark.memory.offHeap.enabled=true \
     --conf spark.memory.offHeap.size=5g \
     --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
     --jars ${GLUTEN_JAR_PATH}"
fi
if [ -f "${NATIVE_LIB_PATH}" ]; then
  if [ "$GLUTEN_EXISTS" == "true" ]; then
    GLUTEN_UDF_EXISTS="true"
    GLUTEN_SPARK_EXTRA="$GLUTEN_SPARK_EXTRA \
      --conf spark.jars=${GLUTEN_JAR_PATH} \
      --conf spark.gluten.loadLibFromJar=true \
      --files ${NATIVE_LIB_PATH} \
      --conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=${GLUTEN_NATIVE_LIB_NAME}"
  fi
fi
SPARK_EXTRA=GLUTEN_SPARK_EXTRA

export SPARK_EXTRA
export GLUTEN_UDF_EXISTS
export GLUTEN_EXISTS
