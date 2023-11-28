#!/bin/bash
set -ex
set -o pipefail

source env_setup.sh

# Check if we gluten and gluten UDFs present
GLUTEN_NATIVE_LIB_NAME=libhigh-performance-spark-gluten-0.so
NATIVE_LIB_DIR=./native/src/
NATIVE_LIB_PATH="${NATIVE_LIB_DIR}${GLUTEN_NATIVE_LIB_NAME}"
GLUTEN_HOME=./gluten
if [ -f "${NATIVE_LIB_PATH}" ]; then
  GLUTEN_EXISTS="true"
  gluten_jvm_jar=$(ls "${GLUTEN_HOME}"/package/target/gluten-velox-bundle-spark3.5_2.12-ubuntu_*-*-SNAPSHOT.jar) #TBD
  gluten_jvm_package_jar=$(ls "${GLUTEN_HOME}"/package/target/gluten-package*-*-SNAPSHOT.jar)
  GLUTEN_SPARK_EXTRA="--conf spark.plugins=io.glutenproject.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --jars ${gluten_jvm_jar},${gluten_jvm_package} \
  --conf spark.jars=${gluten_jvm_jar} \
  --conf spark.driver.extraClassPath=${gluten_jvm_package_jar} \
  --conf spark.executor.extraClassPath=${gluten_jvm_package_jar} \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=${GLUTEN_NATIVE_LIB_NAME} \
  --conf spark.gluten.loadLibFromJar=true \
  --files ${NATIVE_LIB_PATH}"
fi

function run_example () {
  local sql_file="$1"
  local extra="$2"
  # shellcheck disable=SC2046
  spark-sql --master local[5] \
	    --conf spark.eventLog.enabled=true \
	    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
	    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	    --conf spark.sql.catalog.spark_catalog.type=hive \
	    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	    --conf spark.sql.catalog.local.type=hadoop \
	    --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	    ${extra} \
	    $(cat "${sql_file}.conf" || echo "") \
	    --name "${sql_file}" \
	    -f "${sql_file}" | tee -a "${sql_file}.out" || ls "${sql_file}.expected_to_fail"
}


# If you want to look at them
# ${SPARK_PATH}/sbin/start-history-server.sh

if [ $# -eq 1 ]; then
  if [[ "$1" != *"gluten_only"* ]]; then
    run_example "sql/$1"
  else
    echo "Processing gluten ${sql_file}"
    # shellcheck disable=SC2046
    run_example "$sql_file" "$GLUTEN_SPARK_EXTRA"
  fi
else
  # For each SQL
  for sql_file in sql/*.sql; do
    if [[ "$sql_file" != *"gluten_only"* ]]; then
      echo "Processing ${sql_file}"
      # shellcheck disable=SC2046
      run_example "$sql_file"
    elif [[ "$GLUTEN_EXISTS" == "true" ]]; then
      echo "Processing gluten ${sql_file}"
      # shellcheck disable=SC2046
      run_example "$sql_file" "$GLUTEN_SPARK_EXTRA"
    else
      echo "Skipping $sql_file since we did not find gluten and this is a gluten only example."
    fi
  done
fi
