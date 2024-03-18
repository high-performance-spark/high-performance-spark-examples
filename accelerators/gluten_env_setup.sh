#!/bin/bash

# Check if we gluten and gluten UDFs present
GLUTEN_NATIVE_LIB_NAME=libhigh-performance-spark-gluten-0.so
NATIVE_LIB_DIR=$(pwd)/../native/src/
NATIVE_LIB_PATH="${NATIVE_LIB_DIR}${GLUTEN_NATIVE_LIB_NAME}"
GLUTEN_HOME=incubator-gluten
source /etc/lsb-release
if [ "$SPARK_MAJOR" == "3.4" && "$DISTRIB_RELEASE" == "20.04" ]; then
  GLUTEN_EXISTS="true"
  gluten_jvm_jar=$(ls accelerators/gluten-velox-bundle-spark3.4_2.12-1.1.0.jar)
  GLUTEN_SPARK_EXTRA="--conf spark.plugins=io.glutenproject.GlutenPlugin \
     --conf spark.memory.offHeap.enabled=true \
     --conf spark.memory.offHeap.size=5g \
     --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
     --jars ${gluten_jvm_jar}"
else
  if [ -d ${GLUTEN_HOME} ]; then
    GLUTEN_EXISTS="true"
    gluten_jvm_jar=$(ls "${GLUTEN_HOME}"/package/target/gluten-velox-bundle-spark3.5_2.12-ubuntu_*-*-SNAPSHOT.jar) #TBD
    gluten_jvm_package_jar=$(ls "${GLUTEN_HOME}"/package/target/gluten-package*-*-SNAPSHOT.jar)
    GLUTEN_SPARK_EXTRA="--conf spark.plugins=io.glutenproject.GlutenPlugin \
      --jars ${gluten_jvm_jar},${gluten_jvm_package_jar} \
     --conf spark.memory.offHeap.enabled=true \
     --conf spark.memory.offHeap.size=5g \
     --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager"
      # Enable UDF seperately.
  fi
fi
if [ -f "${NATIVE_LIB_PATH}" ]; then
  if [ "$GLUTEN_EXISTS" == "true" ]; then
    GLUTEN_UDF_EXISTS="true"
    GLUTEN_SPARK_EXTRA="$GLUTEN_SPARK_EXTRA \
      --conf spark.jars=${gluten_jvm_jar} \
      --conf spark.gluten.loadLibFromJar=true \
      --files ${NATIVE_LIB_PATH} \
      --conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=${GLUTEN_NATIVE_LIB_NAME}"
  fi
fi
SPARK_EXTRA=GLUTEN_SPARK_EXTRA

export SPARK_EXTRA
export GLUTEN_UDF_EXISTS
export GLUTEN_EXISTS
