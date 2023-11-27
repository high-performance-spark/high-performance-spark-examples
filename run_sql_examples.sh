#!/bin/bash
set -ex
set -o pipefail

source env_setup.sh

# Check if we gluten and gluten UDFs present
GLUTEN_NATIVE_LIB_NAME=libhigh-performance-spark-gluten-0.so
NATIVE_LIB_DIR=./native/src/
NATIVE_LIB_PATH="${NATIVE_LIB_DIR}{$GLUTEN_NATIVE_LIB_NAME}"
# TODO: Check for gluten java libs.
if [ -f "${NATIVE_LIB_PATH}" ]; then
  # TODO: Finish selectively enabling gluten
fi

function run_example () {
  local sql_file="$1"
  # shellcheck disable=SC2046
  spark-sql --master local[5] \
	    --conf spark.eventLog.enabled=true \
	    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
	    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	    --conf spark.sql.catalog.spark_catalog.type=hive \
	    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	    --conf spark.sql.catalog.local.type=hadoop \
	    --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	    ${SPARK_EXTRA} \
	    $(cat "${sql_file}.conf" || echo "") \
	    --name "${sql_file}" \
	    -f "${sql_file}" | tee -a "${sql_file}.out" || ls "${sql_file}.expected_to_fail"
}


# If you want to look at them
# ${SPARK_PATH}/sbin/start-history-server.sh

if [ $# -eq 1 ]; then
  run_example "sql/$1"
else
  # For each SQL
  for sql_file in sql/*.sql; do
    echo "Processing ${sql_file}"
    # shellcheck disable=SC2046
    run_example "$sql_file"
  done
fi
